"""Gold: star schema (Kimball) sobre a Silver — o que o analista e o BI consomem.

Tabelas (todas Delta, sem partição; Liquid Clustering onde ajuda — ver notebook 07):

| tabela | tipo | grão | chaves |
|---|---|---|---|
| `dim_date` | dimensão | 1 linha por dia | `date_key` int yyyymmdd |
| `dim_actor` | dimensão SCD1 | 1 linha por `actor_id` | `actor_sk` = xxhash64(actor_id) |
| `dim_repo` | dimensão SCD2 | 1 linha por versão de repo | `repo_sk` (da SCD2) + membro `-1` desconhecido |
| `fct_events` | fato transacional | 1 linha por evento | `event_id`; FKs `date_key`, `actor_sk`, `repo_sk` |
| `fct_repo_activity_daily` | fato agregado | 1 linha por repo × dia | `date_key`, `repo_id` (chave durável) |

`repo_sk` em `fct_events` é a versão do repositório VIGENTE no instante do evento (join point-in-time
contra a SCD2): um evento de antes de uma renomeação aponta para o nome antigo.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

UNKNOWN_SK = -1
_DIAS = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
_MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]


def date_key(col: str = "event_date") -> F.Column:
    """Chave inteira yyyymmdd — legível, ordenável e barata de comparar."""
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """Dimensão de datas gerada (não vem da fonte): um dia por linha entre `start` e `end`."""
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    iso_dow = ((F.dayofweek("date") + 5) % 7) + 1  # 1 = segunda … 7 = domingo (ISO 8601)
    return days.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.element_at(F.array(*[F.lit(m) for m in _MESES]), F.month("date")).alias("month_name"),
        F.dayofmonth("date").alias("day"),
        iso_dow.alias("day_of_week"),
        F.element_at(F.array(*[F.lit(d) for d in _DIAS]), iso_dow).alias("day_name"),
        (iso_dow >= 6).alias("is_weekend"),
    )


def build_dim_actor(silver: DataFrame) -> DataFrame:
    """SCD1 (sobrescreve): login e flag de bot mais recentes de cada ator."""
    return silver.groupBy("actor_id").agg(
        F.max_by("actor_login", "created_at").alias("actor_login"),
        F.max_by("is_bot", "created_at").alias("is_bot"),
        F.min("created_at").alias("first_seen_at"),
        F.max("created_at").alias("last_seen_at"),
    ).select(F.xxhash64("actor_id").alias("actor_sk"), "*")


def build_dim_repo(spark: SparkSession, repo_scd2: DataFrame) -> DataFrame:
    """Dimensão de repositório a partir da SCD2 + membro 'desconhecido' (sk = -1).

    O membro desconhecido evita FK nula no fato: todo evento aponta para ALGUMA linha da dimensão.
    """
    dim = repo_scd2.select(
        F.col("sk").alias("repo_sk"),
        "repo_id", "repo_name", "repo_owner", "valid_from", "valid_to", "is_current",
    )
    unknown = spark.createDataFrame(
        [(UNKNOWN_SK, UNKNOWN_SK, "(desconhecido)", "(desconhecido)", None, None, True)], dim.schema
    ).withColumn("valid_from", F.lit("1900-01-01 00:00:00").cast("timestamp"))
    return dim.unionByName(unknown)


def build_fct_events(silver: DataFrame, dim_repo: DataFrame, dim_actor: DataFrame) -> DataFrame:
    """Fato transacional, grão = 1 evento. `repo_sk` resolvido por join point-in-time (as-of)."""
    e = silver.alias("e")
    r = dim_repo.filter(F.col("repo_sk") != UNKNOWN_SK).alias("r")
    in_window = (
        (F.col("e.repo_id") == F.col("r.repo_id"))
        & (F.col("e.created_at") >= F.col("r.valid_from"))
        & (F.col("r.valid_to").isNull() | (F.col("e.created_at") < F.col("r.valid_to")))
    )
    a = dim_actor.select("actor_id", "actor_sk")
    return (
        e.join(r, in_window, "left")
        .join(a, "actor_id", "left")
        .select(
            F.col("e.event_id").alias("event_id"),
            date_key("e.event_date").alias("date_key"),
            F.col("e.event_hour").alias("event_hour"),
            F.col("e.created_at").alias("created_at"),
            F.col("e.event_type").alias("event_type"),
            F.col("e.action").alias("action"),
            F.coalesce(F.col("actor_sk"), F.lit(UNKNOWN_SK).cast("bigint")).alias("actor_sk"),
            F.coalesce(F.col("r.repo_sk"), F.lit(UNKNOWN_SK).cast("bigint")).alias("repo_sk"),
            F.col("e.repo_id").alias("repo_id"),
            F.col("e.pr_number").alias("pr_number"),
            F.col("e.issue_number").alias("issue_number"),
            F.col("e.review_state").alias("review_state"),
        )
    )


def _count_if(cond: str) -> F.Column:
    return F.sum(F.when(F.expr(cond), 1).otherwise(0)).cast("int")


def build_fct_repo_activity_daily(silver: DataFrame) -> DataFrame:
    """Fato agregado, grão = repositório × dia. `distinct_actors` NÃO é aditivo entre dias."""
    return silver.groupBy(date_key("event_date").alias("date_key"), "repo_id").agg(
        F.count("*").cast("int").alias("events"),
        _count_if("event_type = 'PushEvent'").alias("pushes"),
        _count_if("event_type = 'PullRequestEvent' AND action = 'opened'").alias("prs_opened"),
        _count_if("event_type = 'PullRequestEvent' AND action = 'merged'").alias("prs_merged"),
        _count_if("event_type = 'PullRequestEvent' AND action = 'closed'").alias("prs_closed_unmerged"),
        _count_if("event_type = 'IssuesEvent' AND action = 'opened'").alias("issues_opened"),
        _count_if("event_type = 'IssuesEvent' AND action = 'closed'").alias("issues_closed"),
        _count_if("event_type = 'WatchEvent'").alias("stars"),
        _count_if("event_type = 'ForkEvent'").alias("forks"),
        _count_if("event_type = 'ReleaseEvent'").alias("releases"),
        _count_if("is_bot").alias("bot_events"),
        F.countDistinct("actor_id").cast("int").alias("distinct_actors"),
    )


def write_table(
    spark: SparkSession, df: DataFrame, path: str, cluster_by: Sequence[str] | None = None
) -> None:
    """Grava (substitui) uma tabela Gold por caminho, com Liquid Clustering opcional.

    Usa SQL `CREATE OR REPLACE TABLE … CLUSTER BY … AS SELECT`: no Delta OSS 4.4,
    `DataFrameWriter.clusterBy(...).save(path)` é ignorado em silêncio (ver notebook 07).
    O REPLACE gera uma nova versão no log (time travel continua valendo).
    """
    view = "_gold_write_" + path.rstrip("/").rsplit("/", 1)[-1]
    df.createOrReplaceTempView(view)
    cluster = f"CLUSTER BY ({', '.join(cluster_by)})" if cluster_by else ""
    spark.sql(f"CREATE OR REPLACE TABLE delta.`{path}` USING delta {cluster} AS SELECT * FROM {view}")
    spark.catalog.dropTempView(view)
