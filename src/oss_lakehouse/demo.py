"""Demonstração de ponta a ponta para mostrar ao vivo: `make demo`.

Roda bronze → silver → gold → quality, registrando cada etapa na tabela de execuções
(`ops/pipeline_runs`), e fecha com três perguntas de negócio respondidas na gold.
Rodar de novo mostra a idempotência: mesmas contagens, etapas mais rápidas.
"""

from __future__ import annotations

from oss_lakehouse import pipeline
from oss_lakehouse.config import get_settings
from oss_lakehouse.observability import DeltaRunSink, new_run_id, ops_path, track_step
from oss_lakehouse.spark import get_spark

PIPELINE = "oss_lakehouse_demo"

QUERIES = {
    "Repositórios mais ativos": """
        SELECT r.repo_name, sum(f.events) AS eventos, sum(f.pushes) AS pushes, sum(f.prs_opened) AS prs
        FROM fct_repo_activity_daily f
        JOIN dim_repo r ON r.repo_id = f.repo_id AND r.is_current
        GROUP BY r.repo_name ORDER BY eventos DESC LIMIT 5""",
    "Participação de bots": """
        SELECT a.is_bot, count(*) AS eventos,
               round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
        FROM fct_events f JOIN dim_actor a USING (actor_sk)
        GROUP BY a.is_bot ORDER BY eventos DESC""",
    "Eventos por hora e tipo (top 3 tipos)": """
        SELECT event_hour, event_type, count(*) AS eventos
        FROM fct_events
        WHERE event_type IN ('PushEvent', 'CreateEvent', 'PullRequestEvent')
        GROUP BY event_hour, event_type ORDER BY event_hour, eventos DESC""",
}


def run() -> None:
    spark = get_spark("demo")
    s = get_settings()
    sink = DeltaRunSink(spark, ops_path("pipeline_runs"))
    run_id = new_run_id()
    steps = {
        "bronze": pipeline.run_bronze,
        "silver": pipeline.run_silver,
        "gold": pipeline.run_gold,
        "quality": pipeline.run_quality,
    }

    print(f"\n== pipeline {PIPELINE} · run_id={run_id} ==")
    for name, step in steps.items():
        with track_step(PIPELINE, name, sink, spark, run_id=run_id) as run_info:
            result = step(spark)
        detail = result if name != "quality" else {"quarentena": result["quarantine"]}
        print(f"  {name:<8} {run_info.duration_s:6.1f}s  {detail}")

    for table in ("dim_repo", "dim_actor", "fct_events", "fct_repo_activity_daily"):
        spark.read.format("delta").load(s.path("gold", table)).createOrReplaceTempView(table)
    for title, sql in QUERIES.items():
        print(f"\n-- {title}")
        spark.sql(sql).show(10, truncate=False)

    print("-- Execuções registradas (ops/pipeline_runs)")
    (
        spark.read.format("delta").load(ops_path("pipeline_runs"))
        .where(f"run_id = '{run_id}'")
        .select("step", "status", "duration_s", "started_at")
        .orderBy("started_at")
        .show(truncate=False)
    )
    spark.stop()
