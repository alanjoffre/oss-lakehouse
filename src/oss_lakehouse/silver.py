"""Silver: eventos do GitHub limpos, tipados, deduplicados e prontos para consumo.

Esquema de `silver/gh_events` (grão: 1 linha por `event_id`; particionada por `event_date`):

| coluna | tipo | origem |
|---|---|---|
| event_id | bigint | `id` (string na bronze) |
| event_type | string | `type` |
| created_at | timestamp (UTC) | `created_at` (string ISO-8601) |
| event_date / event_hour | date / int | derivadas de `created_at` |
| actor_id, actor_login, is_bot | bigint, string, boolean | `actor` achatado; bot = login termina em `[bot]` |
| repo_id, repo_name, repo_owner | bigint, string, string | `repo` achatado; dono = parte antes da `/` |
| org_id, org_login | bigint, string | `org` (nulo em repo pessoal) |
| action, ref, ref_type, push_id, head_sha | | campos do `payload` (JSON) |
| pr_number, pr_id, pr_base_ref, pr_head_ref | | eventos de PR/review |
| issue_number, issue_title, issue_state | | IssuesEvent / IssueCommentEvent |
| review_state, release_tag | | review / release |
| is_public | boolean | `public` |
| _source_file, _ingested_at | string, timestamp | linhagem vinda da bronze |
| _content_hash | string | sha256 do conteúdo do evento — decide se o MERGE precisa reescrever |
| _processed_at | timestamp | quando a Silver gravou a versão |

Observação sobre o formato do GH Archive em 2026: o payload de `PullRequestEvent` é enxuto
(sem `title`/`merged`); o merge aparece como `action = 'merged'`.
"""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import LongType, StringType, StructField, StructType

from oss_lakehouse.config import get_settings
from oss_lakehouse.scd2 import merge_metrics_since, table_version

SILVER_TABLE = "gh_events"
BOT_LOGIN_REGEX = r"(?i)\[bot\]$"

# Schema PARCIAL do payload: só os campos que a Silver usa. `from_json` faz UM parse por linha
# e ignora o resto do JSON — mais barato que N chamadas a `get_json_object` (1 parse cada).
PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType()),
        StructField("ref", StringType()),
        StructField("ref_type", StringType()),
        StructField("push_id", LongType()),
        StructField("head", StringType()),
        StructField("number", LongType()),
        StructField(
            "pull_request",
            StructType(
                [
                    StructField("id", LongType()),
                    StructField("number", LongType()),
                    StructField("base", StructType([StructField("ref", StringType())])),
                    StructField("head", StructType([StructField("ref", StringType())])),
                ]
            ),
        ),
        StructField(
            "issue",
            StructType(
                [
                    StructField("number", LongType()),
                    StructField("title", StringType()),
                    StructField("state", StringType()),
                ]
            ),
        ),
        StructField("review", StructType([StructField("state", StringType())])),
        StructField("release", StructType([StructField("tag_name", StringType())])),
    ]
)


def content_hash(*cols: str | Column) -> Column:
    """sha256 do conteúdo. `to_json(struct)` preserva nulos (concat_ws os pularia e colidiria)."""
    return F.sha2(F.to_json(F.struct(*cols)), 256)


def bronze_to_silver(bronze: DataFrame) -> DataFrame:
    """Tipagem + achatamento + extração do payload. Transformação pura (não deduplica)."""
    p = F.from_json(F.col("payload"), PAYLOAD_SCHEMA)
    created = F.to_timestamp(F.col("created_at"))
    return (
        bronze.withColumn("_p", p)
        .withColumn("_ts", created)
        .select(
            F.col("id").cast("bigint").alias("event_id"),
            F.col("type").alias("event_type"),
            F.col("_ts").alias("created_at"),
            F.to_date("_ts").alias("event_date"),
            F.hour("_ts").alias("event_hour"),
            F.col("actor.id").alias("actor_id"),
            F.col("actor.login").alias("actor_login"),
            F.coalesce(F.col("actor.login").rlike(BOT_LOGIN_REGEX), F.lit(False)).alias("is_bot"),
            F.col("repo.id").alias("repo_id"),
            F.col("repo.name").alias("repo_name"),
            F.split_part(F.col("repo.name"), F.lit("/"), F.lit(1)).alias("repo_owner"),
            F.col("org.id").alias("org_id"),
            F.col("org.login").alias("org_login"),
            F.col("_p.action").alias("action"),
            F.col("_p.ref").alias("ref"),
            F.col("_p.ref_type").alias("ref_type"),
            F.col("_p.push_id").alias("push_id"),
            F.col("_p.head").alias("head_sha"),
            F.coalesce(F.col("_p.number"), F.col("_p.pull_request.number")).cast("int").alias("pr_number"),
            F.col("_p.pull_request.id").alias("pr_id"),
            F.col("_p.pull_request.base.ref").alias("pr_base_ref"),
            F.col("_p.pull_request.head.ref").alias("pr_head_ref"),
            F.col("_p.issue.number").cast("int").alias("issue_number"),
            F.col("_p.issue.title").alias("issue_title"),
            F.col("_p.issue.state").alias("issue_state"),
            F.col("_p.review.state").alias("review_state"),
            F.col("_p.release.tag_name").alias("release_tag"),
            F.col("public").alias("is_public"),
            F.col("_source_file"),
            F.col("_ingested_at"),
            content_hash("id", "type", "actor", "repo", "org", "payload", "public", "created_at").alias(
                "_content_hash"
            ),
            F.current_timestamp().alias("_processed_at"),
        )
    )


def deduplicate_latest(df: DataFrame, keys: Sequence[str], order_by: Sequence[Column]) -> DataFrame:
    """Uma linha por chave, a PRIMEIRA segundo `order_by` (ex.: `_ingested_at` desc = a mais recente).

    Por que não `dropDuplicates(keys)`: ele mantém uma linha QUALQUER — a escolha depende da ordem
    das partições e muda entre execuções. `row_number` com desempate explícito é determinístico.
    """
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def dedup_events(df: DataFrame) -> DataFrame:
    """Regra da Silver: por `event_id`, fica a cópia ingerida por último (desempate pelo arquivo)."""
    return deduplicate_latest(df, ["event_id"], [F.col("_ingested_at").desc(), F.col("_source_file").desc()])


def ensure_silver_table(spark: SparkSession, path: str, schema: StructType) -> None:
    """Cria a tabela (vazia) se não existir: particionada por data, deletion vectors ligados."""
    (
        DeltaTable.createIfNotExists(spark)
        .location(path)
        .addColumns(schema)
        .partitionedBy("event_date")
        .property("delta.enableDeletionVectors", "true")
        .execute()
    )


def merge_events(spark: SparkSession, updates: DataFrame, target_path: str) -> dict[str, str]:
    """Upsert idempotente na Silver. Devolve as métricas da operação (do `DESCRIBE HISTORY`).

    - chave de casamento: `event_id` + `event_date` (a data nunca muda para um evento);
    - as datas do lote entram como LITERAIS na condição → o Delta poda partições/arquivos do alvo
      antes do join (sem isso o MERGE lê a tabela inteira para procurar casamentos);
    - só reescreve linha cujo `_content_hash` mudou: reprocessar o mesmo dado não toca o alvo.
    """
    ensure_silver_table(spark, target_path, updates.schema)
    dates = sorted({str(r[0]) for r in updates.select("event_date").distinct().collect() if r[0] is not None})
    if not dates:
        return {}
    date_list = ", ".join(f"DATE'{d}'" for d in dates)
    cond = f"t.event_date IN ({date_list}) AND t.event_date = s.event_date AND t.event_id = s.event_id"
    target = DeltaTable.forPath(spark, target_path)
    v0 = table_version(target)
    (
        target.alias("t")
        .merge(updates.alias("s"), cond)
        .whenMatchedUpdateAll(condition="t._content_hash <> s._content_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return merge_metrics_since(target, v0)


def build_silver_events(
    spark: SparkSession,
    bronze_path: str | None = None,
    silver_path: str | None = None,
    checkpoint_path: str | None = None,
) -> StreamingQuery:
    """Bronze → Silver incremental: lê só as versões novas da bronze (stream Delta + checkpoint),
    e em cada micro-lote tipa, deduplica e faz MERGE (`foreachBatch`). Para quando acabar (availableNow).
    """
    s = get_settings()
    bronze_path = bronze_path or s.path("bronze", "gh_events")
    silver_path = silver_path or s.path("silver", SILVER_TABLE)
    checkpoint_path = checkpoint_path or s.checkpoint("silver_gh_events")

    def _upsert(batch: DataFrame, batch_id: int) -> None:
        # O lote é usado mais de uma vez (datas distintas para a poda + MERGE): sem `persist`, a bronze
        # seria relida e retransformada a cada uso.
        updates = dedup_events(bronze_to_silver(batch)).persist()
        try:
            merge_events(batch.sparkSession, updates, silver_path)
        finally:
            updates.unpersist()

    query = (
        spark.readStream.format("delta")
        .load(bronze_path)
        .writeStream.foreachBatch(_upsert)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return query
