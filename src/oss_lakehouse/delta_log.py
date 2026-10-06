"""Delta Lake por dentro: ler o transaction log à mão, tratar conflito e consumir o Change Data Feed.

Três grupos de ferramentas, usadas no notebook 10:

- **Log** (`read_commit`, `summarize_commit`, `log_summary`): abre os JSON de `_delta_log/` sem
  passar pelo Spark. Serve para PROVAR o que cada operação gravou (quantos `add`/`remove`, se
  houve deletion vector, se o commit mudou dado ou só reorganizou arquivos).
- **Concorrência** (`retry_on_conflict`): repete uma escrita que perdeu a corrida do controle de
  concorrência otimista. É o tratamento padrão: a exceção de conflito é *retryable* por desenho.
- **CDF** (`apply_changes`): consumidor incremental em lote do Change Data Feed — lê só as
  versões novas, reduz a uma mudança por chave e aplica com MERGE na tabela de destino.

Limite assumido: caminhos locais (ou montados). Em `abfss://` o log é lido pelo próprio Delta
(`DESCRIBE HISTORY`, `DeltaTable.history()`); abrir o JSON à mão é ferramenta de estudo e diagnóstico.
"""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ACTION_TYPES = ("commitInfo", "protocol", "metaData", "add", "remove", "cdc", "txn", "domainMetadata")
CDF_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")

# ---------------------------------------------------------------------------------------------
# Transaction log
# ---------------------------------------------------------------------------------------------


def log_dir(table_path: str) -> Path:
    """Pasta `_delta_log` da tabela, como `Path` local (aceita o prefixo `file:`)."""
    return Path(table_path.removeprefix("file:")) / "_delta_log"


def commit_versions(table_path: str) -> list[int]:
    """Versões que ainda têm o JSON de commit no log (as antigas somem depois da retenção)."""
    return sorted(int(p.stem) for p in log_dir(table_path).glob("*.json") if p.stem.isdigit())


def read_commit(table_path: str, version: int) -> list[dict[str, Any]]:
    """Ações de um commit: cada linha do `<versão com 20 dígitos>.json` é uma ação."""
    path = log_dir(table_path) / f"{version:020d}.json"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


@dataclass(frozen=True)
class CommitSummary:
    """O que um commit fez, em números. `data_change=False` = só reorganizou (OPTIMIZE)."""

    version: int
    operation: str
    actions: dict[str, int]
    files_added: int
    files_removed: int
    bytes_added: int
    bytes_new: int
    adds_with_dv: int
    cdc_files: int
    data_change: bool

    def as_row(self) -> dict[str, Any]:
        return {
            "versão": self.version,
            "operação": self.operation,
            "add": self.files_added,
            "remove": self.files_removed,
            "add c/ DV": self.adds_with_dv,
            "cdc": self.cdc_files,
            "KB em add": round(self.bytes_added / 1024, 1),
            "KB novos": round(self.bytes_new / 1024, 1),
            "dataChange": self.data_change,
        }


def summarize_commit(actions: Sequence[dict[str, Any]], version: int = -1) -> CommitSummary:
    """Resume as ações de um commit (o que `DESCRIBE HISTORY` mostra, mas contado do JSON).

    `bytes_added` soma o tamanho de todo arquivo referenciado por `add` (e `cdc`);
    `bytes_new` desconta o `add` cujo caminho também aparece num `remove` do mesmo commit —
    é o caso do deletion vector: o MESMO parquet é re-adicionado com um DV, nada é reescrito.
    """
    counts: Counter[str] = Counter()
    operation = ""
    bytes_added = bytes_new = adds_with_dv = 0
    data_change = False
    removed_paths = {a["remove"]["path"] for a in actions if "remove" in a}
    for action in actions:
        (kind, body), *_ = action.items()
        counts[kind] += 1
        if kind == "commitInfo":
            operation = body.get("operation", "")
        elif kind == "add":
            bytes_added += body.get("size", 0)
            bytes_new += 0 if body.get("path") in removed_paths else body.get("size", 0)
            adds_with_dv += 1 if body.get("deletionVector") else 0
            data_change = data_change or bool(body.get("dataChange", True))
        elif kind == "remove":
            data_change = data_change or bool(body.get("dataChange", True))
        elif kind == "cdc":
            bytes_added += body.get("size", 0)
            bytes_new += body.get("size", 0)
    return CommitSummary(
        version=version,
        operation=operation,
        actions=dict(counts),
        files_added=counts["add"],
        files_removed=counts["remove"],
        bytes_added=bytes_added,
        bytes_new=bytes_new,
        adds_with_dv=adds_with_dv,
        cdc_files=counts["cdc"],
        data_change=data_change,
    )


def log_summary(table_path: str, last: int | None = None) -> list[CommitSummary]:
    """Resumo de cada commit do log, em ordem (`last=N` devolve só os N mais recentes)."""
    versions = commit_versions(table_path)
    if last is not None:
        versions = versions[-last:]
    return [summarize_commit(read_commit(table_path, v), v) for v in versions]


def physical_files(table_path: str) -> dict[str, int]:
    """Quantos arquivos existem DE FATO na pasta da tabela, por tipo (fora do `_delta_log`).

    O log diz o que está *ativo*; o disco guarda também o que já foi removido logicamente e
    ainda não passou pelo VACUUM. A diferença entre os dois é o custo do time travel.
    """
    root = Path(table_path.removeprefix("file:"))
    out = {"parquet": 0, "deletion_vector": 0, "cdc": 0}
    for path in root.rglob("*"):
        rel = path.relative_to(root).parts
        if not path.is_file() or rel[0] == "_delta_log" or path.name.startswith("."):
            continue
        if rel[0] == "_change_data":
            out["cdc"] += 1
        elif path.name.startswith("deletion_vector") and path.suffix == ".bin":
            out["deletion_vector"] += 1
        elif path.suffix == ".parquet":
            out["parquet"] += 1
    return out


# ---------------------------------------------------------------------------------------------
# Concorrência otimista: tentar de novo
# ---------------------------------------------------------------------------------------------


def conflict_exceptions() -> tuple[type[BaseException], ...]:
    """Exceções de conflito do Delta que fazem sentido repetir.

    `MetadataChangedException` e `ProtocolChangedException` ficam de fora de propósito: alguém
    mudou o schema ou o protocolo no meio — repetir às cegas pode gravar com o schema errado.
    """
    from delta import exceptions as dex

    return (
        dex.ConcurrentAppendException,
        dex.ConcurrentDeleteReadException,
        dex.ConcurrentDeleteDeleteException,
        dex.ConcurrentWriteException,
    )


def retry_on_conflict[T](
    action: Callable[[], T],
    attempts: int = 3,
    base_delay: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
    retry_on: tuple[type[BaseException], ...] | None = None,
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
    """Executa `action`; se perder a corrida do commit, espera (backoff exponencial) e repete.

    `action` precisa refazer a operação inteira (reler o snapshot novo): por isso recebe uma
    função, não um DataFrame já calculado. Só é seguro com operação idempotente (MERGE por
    chave, DELETE/UPDATE por predicado). Append cego repetido duplica dado — e nem conflita.
    """
    if attempts < 1:
        raise ValueError("attempts precisa ser >= 1")
    errors = retry_on if retry_on is not None else conflict_exceptions()
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except errors as exc:
            if attempt == attempts:
                raise
            if on_retry:
                on_retry(attempt, exc)
            sleep(base_delay * 2 ** (attempt - 1))
    raise AssertionError("inalcançável")  # pragma: no cover


# ---------------------------------------------------------------------------------------------
# Change Data Feed: consumidor incremental em lote
# ---------------------------------------------------------------------------------------------


def latest_version(spark: SparkSession, table_path: str) -> int:
    """Última versão commitada da tabela, via `DESCRIBE HISTORY` (passa pelo Spark, não lê o log à mão)."""
    row = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}` LIMIT 1").select("version").first()
    assert row is not None
    return int(row[0])


def read_changes(spark: SparkSession, table_path: str, start: int, end: int | None = None) -> DataFrame:
    """Mudanças linha a linha entre as versões `start` e `end` (inclusive) — exige CDF ligado."""
    reader = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start)
    if end is not None:
        reader = reader.option("endingVersion", end)
    return reader.load(table_path)


def latest_change_per_key(changes: DataFrame, key: str) -> DataFrame:
    """Reduz o feed a UMA linha por chave: o estado final da chave no intervalo lido.

    - `update_preimage` (o valor antigo) é descartado: para replicar basta o valor novo;
    - vale a mudança do commit mais recente; no empate dentro do mesmo commit (ex.: overwrite
      apaga e reinsere a chave), a linha que existe ganha do `delete`.
    Sem isso o MERGE falharia: duas linhas da origem casando com a mesma linha do destino.
    """
    w = Window.partitionBy(key).orderBy(
        F.col("_commit_version").desc(),
        F.when(F.col("_change_type") == "delete", 1).otherwise(0),
    )
    return (
        changes.filter(F.col("_change_type") != "update_preimage")
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )


@dataclass(frozen=True)
class CdfRun:
    """Resultado de uma rodada do consumidor."""

    start_version: int
    end_version: int
    changes_read: int
    keys_applied: int

    @property
    def skipped(self) -> bool:
        return self.changes_read == 0 and self.end_version < self.start_version


def _read_state(state_file: Path) -> int | None:
    if not state_file.exists():
        return None
    return int(json.loads(state_file.read_text())["last_version"])


def _write_state(state_file: Path, version: int) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(json.dumps({"last_version": version}))
    os.replace(tmp, state_file)  # troca atômica: nunca fica um estado pela metade


def apply_changes(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    key: str,
    state_file: str | Path,
    first_version: int = 0,
) -> CdfRun:
    """Replica `source` em `target` lendo só as versões que ainda não foram processadas.

    1. lê de `state_file` a última versão aplicada (marca d'água por versão do Delta);
    2. lê o Change Data Feed de `última + 1` até a versão atual da origem;
    3. reduz a uma mudança por chave e aplica com MERGE (delete / update / insert);
    4. só então grava a nova marca. Se cair entre 3 e 4, a próxima rodada reaplica o mesmo
       intervalo — e o MERGE por chave dá o mesmo resultado (idempotente): *at-least-once* na
       leitura + escrita idempotente = efeito *exactly-once*.
    """
    from delta.tables import DeltaTable

    state = Path(state_file)
    last = _read_state(state)
    start = first_version if last is None else last + 1
    end = latest_version(spark, source_path)
    if end < start:
        return CdfRun(start, end, 0, 0)

    changes = read_changes(spark, source_path, start, end)
    data_cols = [c for c in changes.columns if c not in CDF_COLUMNS]
    if not DeltaTable.isDeltaTable(spark, target_path):
        changes.select(*data_cols).limit(0).write.format("delta").save(target_path)

    latest = latest_change_per_key(changes, key).cache()
    try:
        changes_read = changes.count()
        keys_applied = latest.count()
        (
            DeltaTable.forPath(spark, target_path)
            .alias("t")
            .merge(latest.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedDelete(condition="s._change_type = 'delete'")
            .whenMatchedUpdate(set={c: f"s.{c}" for c in data_cols})
            .whenNotMatchedInsert(
                condition="s._change_type != 'delete'", values={c: f"s.{c}" for c in data_cols}
            )
            .execute()
        )
    finally:
        latest.unpersist()
    _write_state(state, end)
    return CdfRun(start, end, changes_read, keys_applied)
