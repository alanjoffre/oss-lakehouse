"""SCD tipo 2 genérico com MERGE do Delta — idempotente e tolerante a dado fora de ordem.

SCD (*Slowly Changing Dimension*, dimensão que muda devagar) tipo 2 guarda o HISTÓRICO:
cada mudança de um atributo rastreado fecha a versão atual (`valid_to`) e abre outra.

Colunas que a tabela-alvo ganha além das chaves e atributos rastreados:
`<sk_col>` (surrogate key determinística), `valid_from`, `valid_to` (nulo = vigente),
`is_current`, `_row_hash` e `_updated_at`.

Estratégia para dado atrasado (*late-arriving*): o MERGE "clássico" só sabe fechar a versão
vigente e abrir uma nova — se chega uma observação ANTERIOR à vigente, ele corrompe o histórico.
Aqui, para as chaves presentes no lote, a linha do tempo é RECALCULADA:

    histórico atual das chaves afetadas  ∪  observações novas
        → ordena por data efetiva → descarta observação igual à anterior (sem mudança)
        → valid_to = próxima data efetiva (lead) → MERGE: insere / atualiza / apaga versões

Como a surrogate key é hash(chave natural, valid_from), rodar o mesmo lote 2x produz exatamente
as mesmas linhas — o MERGE não encontra nada para mudar (idempotência).

Observação com chave natural ou data efetiva NULA é descartada: `NULL = NULL` não casa na condição
do MERGE, então ela seria reinserida a cada execução (achado no dado real: um ForkEvent com
`repo: {}`). Medir/quarentenar essas linhas é papel da camada de qualidade (notebook 08).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

META_COLS = ("valid_from", "valid_to", "is_current", "_row_hash", "_updated_at")


def _row_hash(tracked: Sequence[str]) -> Column:
    return F.sha2(F.to_json(F.struct(*tracked)), 256)


def _table_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def table_version(table: DeltaTable) -> int:
    return int(table.history(1).select("version").collect()[0][0])


def merge_metrics_since(table: DeltaTable, version_before: int) -> dict[str, str]:
    """Métricas do MERGE recém-executado. MERGE que não muda nada NÃO grava versão nova no log —
    nesse caso devolve contadores zerados (ler `history(1)` às cegas mostraria a operação anterior).
    """
    last = table.history(1).select("version", "operation", "operationMetrics").collect()[0]
    if int(last["version"]) == version_before:
        zeros = ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")
        return {"operation": "MERGE (sem mudança, nenhuma versão nova)", **dict.fromkeys(zeros, "0")}
    return {"operation": last["operation"], **dict(last["operationMetrics"])}


def build_scd2_history(
    observations: DataFrame,
    keys: Sequence[str],
    tracked: Sequence[str],
    effective_col: str,
    existing: DataFrame | None = None,
    sk_col: str = "sk",
) -> DataFrame:
    """Linha do tempo SCD2 das chaves em `observations` (+ histórico `existing`, se houver).

    Pura (sem I/O): é o coração do algoritmo e o que os testes exercitam.
    Empate de data efetiva para a mesma chave: vence a observação NOVA; entre observações novas,
    o maior hash (arbitrário, mas determinístico).
    """
    keys, tracked = list(keys), list(tracked)
    inc = observations.select(*keys, *tracked, F.col(effective_col).cast("timestamp").alias("valid_from"))
    inc = inc.dropna(subset=[*keys, "valid_from"])  # chave nula nunca casa no MERGE (ver docstring)
    inc = inc.withColumn("_row_hash", _row_hash(tracked)).withColumn("_prio", F.lit(1))
    combined = inc
    if existing is not None:
        old = existing.select(*keys, *tracked, "valid_from", "_row_hash").withColumn("_prio", F.lit(0))
        combined = old.unionByName(inc)

    w_tie = Window.partitionBy(*keys, "valid_from").orderBy(F.col("_prio").desc(), F.col("_row_hash").desc())
    w = Window.partitionBy(*keys).orderBy("valid_from")
    return (
        combined.withColumn("_rn", F.row_number().over(w_tie))
        .filter("_rn = 1")
        .withColumn("_prev_hash", F.lag("_row_hash").over(w))
        .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash")))
        .withColumn("valid_to", F.lead("valid_from").over(w))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn(sk_col, F.xxhash64(*keys, "valid_from"))
        .select(sk_col, *keys, *tracked, "valid_from", "valid_to", "is_current", "_row_hash")
    )


def apply_scd2(
    spark: SparkSession,
    observations: DataFrame,
    target_path: str,
    keys: Sequence[str],
    tracked: Sequence[str],
    effective_col: str,
    sk_col: str = "sk",
) -> dict[str, str]:
    """Aplica `observations` (chave, atributos, data efetiva) na SCD2 em `target_path`.

    Devolve as métricas do MERGE (linhas inseridas/atualizadas/apagadas). Idempotente.
    """
    keys, tracked = list(keys), list(tracked)
    if not _table_exists(spark, target_path):
        history = build_scd2_history(observations, keys, tracked, effective_col, sk_col=sk_col)
        (
            history.withColumn("_updated_at", F.current_timestamp())
            .write.format("delta")
            .option("delta.enableDeletionVectors", "true")
            .save(target_path)
        )
        n = spark.read.format("delta").load(target_path).count()
        return {"operation": "CREATE", "numTargetRowsInserted": str(n)}

    target = DeltaTable.forPath(spark, target_path)
    v0 = table_version(target)
    affected = observations.select(*keys).distinct()
    existing = target.toDF().join(affected, keys, "left_semi")
    history = build_scd2_history(observations, keys, tracked, effective_col, existing, sk_col)

    # Versões antigas que sumiram da linha do tempo recalculada (ex.: dado atrasado tornou uma
    # mudança redundante) viram linhas de DELETE na fonte do MERGE.
    to_delete = existing.join(history.select(*keys, "valid_from"), [*keys, "valid_from"], "left_anti")
    source = history.withColumn("_delete", F.lit(False)).unionByName(
        to_delete.select(*keys, "valid_from").withColumn("_delete", F.lit(True)), allowMissingColumns=True
    )

    on = " AND ".join([f"t.{k} = s.{k}" for k in keys] + ["t.valid_from = s.valid_from"])
    cols = [sk_col, *keys, *tracked, "valid_from", "valid_to", "is_current", "_row_hash"]
    values = {c: f"s.{c}" for c in cols} | {"_updated_at": "current_timestamp()"}
    changed = "t._row_hash <> s._row_hash OR NOT (t.valid_to <=> s.valid_to) OR t.is_current <> s.is_current"
    (
        target.alias("t")
        .merge(source.alias("s"), on)
        .whenMatchedDelete(condition="s._delete")
        .whenMatchedUpdate(condition=f"NOT s._delete AND ({changed})", set=values)
        .whenNotMatchedInsert(condition="NOT s._delete", values=values)
        .execute()
    )
    return merge_metrics_since(target, v0)


def scd2_as_of(dim: DataFrame, as_of: datetime | str) -> DataFrame:
    """Consulta *point-in-time*: a versão vigente de cada chave no instante `as_of`.

    `as_of` em texto é lido no fuso da sessão Spark (UTC neste projeto). `datetime` sem fuso segue a
    convenção do PySpark (fuso local do driver) — prefira texto ou `datetime` com `tzinfo`.
    """
    ts = F.lit(as_of).cast("timestamp")
    return dim.filter((F.col("valid_from") <= ts) & (F.col("valid_to").isNull() | (F.col("valid_to") > ts)))
