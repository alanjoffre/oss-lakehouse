from __future__ import annotations

from datetime import datetime

from chispa import assert_df_equality
from pyspark.sql import functions as F

from oss_lakehouse.bronze import GH_EVENT_SCHEMA, add_ingestion_metadata, ingest_gharchive_bronze
from oss_lakehouse.silver import (
    bronze_to_silver,
    build_silver_events,
    dedup_events,
    deduplicate_latest,
    merge_events,
)


def _bronze_batch(spark, gh_sample_dir):
    raw = spark.read.schema(GH_EVENT_SCHEMA).json(str(gh_sample_dir)).select("*", "_metadata")
    return add_ingestion_metadata(raw).drop("_metadata")


def test_bronze_to_silver_tipa_e_extrai_payload(spark, gh_sample_dir):
    s = bronze_to_silver(_bronze_batch(spark, gh_sample_dir)).cache()
    types = dict(s.dtypes)
    assert types["event_id"] == "bigint" and types["created_at"] == "timestamp"
    assert types["event_hour"] == "int" and types["is_bot"] == "boolean"
    assert s.count() == 2000
    assert s.filter("event_id IS NULL OR created_at IS NULL").count() == 0
    # is_bot = login termina em [bot]; nunca nulo
    assert s.filter("is_bot IS NULL").count() == 0
    assert s.filter("actor_login = 'github-actions[bot]' AND NOT is_bot").count() == 0
    # dono = parte antes da barra
    assert s.filter("repo_owner <> split_part(repo_name, '/', 1)").count() == 0
    # todo PullRequestEvent tem número de PR; push tem ref e push_id
    assert s.filter("event_type = 'PullRequestEvent' AND pr_number IS NULL").count() == 0
    assert s.filter("event_type = 'PushEvent' AND (ref IS NULL OR push_id IS NULL)").count() == 0


def test_deduplicate_latest_fica_com_a_copia_mais_recente(spark):
    rows = [
        (1, "a", datetime(2026, 10, 1, 12), "f1"),
        (1, "b", datetime(2026, 10, 1, 13), "f2"),  # mais recente: fica
        (2, "c", datetime(2026, 10, 1, 12), "f1"),
    ]
    df = spark.createDataFrame(rows, "event_id long, v string, _ingested_at timestamp, _source_file string")
    got = deduplicate_latest(df, ["event_id"], [F.col("_ingested_at").desc()]).orderBy("event_id")
    expected = spark.createDataFrame([rows[1], rows[2]], df.schema)
    assert_df_equality(got, expected)


def test_dedup_events_desempata_pelo_arquivo_de_forma_deterministica(spark):
    ts = datetime(2026, 10, 1, 12)
    df = spark.createDataFrame(
        [(1, "x", ts, "f1"), (1, "y", ts, "f2")],
        "event_id long, v string, _ingested_at timestamp, _source_file string",
    )
    assert [r.v for r in dedup_events(df).collect()] == ["y"]


def test_merge_e_idempotente_e_so_reescreve_o_que_mudou(spark, gh_sample_dir, tmp_path):
    target = str(tmp_path / "silver")
    batch = dedup_events(bronze_to_silver(_bronze_batch(spark, gh_sample_dir))).cache()

    m1 = merge_events(spark, batch, target)
    assert int(m1["numTargetRowsInserted"]) == 2000

    m2 = merge_events(spark, batch, target)  # reprocessar o MESMO lote
    assert int(m2["numTargetRowsInserted"]) == 0 and int(m2["numTargetRowsUpdated"]) == 0
    assert spark.read.format("delta").load(target).count() == 2000

    # um evento corrigido na fonte (hash diferente) é atualizado, não duplicado
    one = batch.limit(1).withColumn("_content_hash", F.lit("novo")).withColumn("action", F.lit("corrigido"))
    m3 = merge_events(spark, one, target)
    assert int(m3["numTargetRowsUpdated"]) == 1 and int(m3["numTargetRowsInserted"]) == 0
    out = spark.read.format("delta").load(target)
    assert out.count() == 2000 and out.filter("action = 'corrigido'").count() == 1


def test_build_silver_events_incremental_sem_duplicar(spark, gh_sample_dir, tmp_path):
    bronze, silver = str(tmp_path / "bronze"), str(tmp_path / "silver")
    ingest_gharchive_bronze(spark, str(gh_sample_dir), bronze, str(tmp_path / "ck_b"))
    build_silver_events(spark, bronze, silver, str(tmp_path / "ck_s"))
    assert spark.read.format("delta").load(silver).count() == 2000
    # reprocessamento total (checkpoint novo = relê a bronze inteira): o MERGE impede duplicata
    build_silver_events(spark, bronze, silver, str(tmp_path / "ck_s2"))
    out = spark.read.format("delta").load(silver)
    assert out.count() == 2000 and out.select("event_id").distinct().count() == 2000
