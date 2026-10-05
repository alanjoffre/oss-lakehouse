"""Etapas do pipeline, uma função por tarefa do job (bronze → silver → gold → quality).

É a cola entre os módulos de cada camada. O mesmo código é chamado pelo `cli` local,
pelo `make demo` e pelas tarefas do job no Databricks (`resources/jobs.yml`).
Todas as etapas são idempotentes: rodar duas vezes deixa as tabelas no mesmo estado.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from oss_lakehouse import gold, quality
from oss_lakehouse.bronze import BRONZE_TABLE, ingest_gharchive_bronze
from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.scd2 import apply_scd2
from oss_lakehouse.silver import SILVER_TABLE, build_silver_events

DIM_REPO_SCD2 = "dim_repo_scd2"
QUARANTINE_TABLE = "silver_gh_events"
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "silver_gh_events.yaml"


def _count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def run_bronze(spark: SparkSession) -> int:
    """Landing → bronze (incremental por checkpoint). Devolve o total de linhas da tabela."""
    ingest_gharchive_bronze(spark)
    return _count(spark, get_settings().path("bronze", BRONZE_TABLE))


def run_silver(spark: SparkSession) -> dict[str, int]:
    """Bronze → silver tipada e deduplicada (MERGE) + dimensão de repositório em SCD tipo 2."""
    s = get_settings()
    build_silver_events(spark)
    silver_path = s.path("silver", SILVER_TABLE)
    observations = spark.read.format("delta").load(silver_path).select(
        "repo_id", "repo_name", "repo_owner", "created_at"
    )
    scd2_path = s.path("silver", DIM_REPO_SCD2)
    apply_scd2(
        spark, observations, scd2_path,
        keys=["repo_id"], tracked=["repo_name", "repo_owner"], effective_col="created_at",
    )
    return {SILVER_TABLE: _count(spark, silver_path), DIM_REPO_SCD2: _count(spark, scd2_path)}


def run_gold(spark: SparkSession) -> dict[str, int]:
    """Silver → star schema (dimensões + fatos), com Liquid Clustering nos fatos."""
    s = get_settings()
    silver = spark.read.format("delta").load(s.path("silver", SILVER_TABLE))
    repo_scd2 = spark.read.format("delta").load(s.path("silver", DIM_REPO_SCD2))

    bounds = silver.agg(F.min("event_date").alias("lo"), F.max("event_date").alias("hi")).first()
    lo, hi = bounds["lo"], bounds["hi"]
    dim_repo = gold.build_dim_repo(spark, repo_scd2)
    dim_actor = gold.build_dim_actor(silver)
    tables = {
        "dim_date": (gold.build_dim_date(spark, date(lo.year, 1, 1), date(hi.year, 12, 31)), None),
        "dim_actor": (dim_actor, None),
        "dim_repo": (dim_repo, ["repo_id"]),
        "fct_events": (gold.build_fct_events(silver, dim_repo, dim_actor), ["date_key", "repo_id"]),
        "fct_repo_activity_daily": (gold.build_fct_repo_activity_daily(silver), ["date_key", "repo_id"]),
    }
    counts: dict[str, int] = {}
    for name, (df, cluster_by) in tables.items():
        path = s.path("gold", name)
        gold.write_table(spark, df, path, cluster_by=cluster_by)
        counts[name] = _count(spark, path)
    return counts


def run_quality(spark: SparkSession, contract_path: str | Path = CONTRACT_PATH) -> dict[str, object]:
    """Valida a silver contra o contrato e aplica as expectations.

    Schema fora do contrato levanta `ContractViolation` e regra `fail` violada levanta
    `ExpectationFailed`: o job para. Linhas reprovadas em regra `drop` vão para a quarentena.
    """
    s = get_settings()
    silver = spark.read.format("delta").load(s.path("silver", SILVER_TABLE))
    contract = quality.load_contract(contract_path)
    quality.enforce_contract(silver, contract)
    result = quality.apply_expectations(silver, quality.expectations_from_dicts(contract.expectations))
    quarantine_path = s.path("quarantine", QUARANTINE_TABLE)
    result.quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        quarantine_path
    )
    return {"quarantine": _count(spark, quarantine_path), "rules": result.metrics}
