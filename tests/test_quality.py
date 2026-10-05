from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from oss_lakehouse.quality import (
    ContractViolation,
    Expectation,
    ExpectationFailed,
    apply_expectations,
    check_freshness,
    enforce_contract,
    expectations_from_dicts,
    load_contract,
    validate_data,
    validate_schema,
    volume_anomalies,
)

CONTRACT = Path(__file__).parents[1] / "contracts" / "silver_gh_events.yaml"


@pytest.fixture
def df(spark):
    rows = [(1, "PushEvent", 10), (2, "PushEvent", None), (3, "Esquisito", 5), (None, "PushEvent", 1)]
    return spark.createDataFrame(rows, "event_id long, event_type string, repo_id long")


def test_warn_mede_mas_nao_remove(df):
    r = apply_expectations(df, [Expectation("tipo_conhecido", "event_type LIKE '%Event'", "warn")])
    assert r.valid.count() == 4 and r.quarantine.count() == 0
    assert r.metrics[0]["failed"] == 1 and r.metrics[0]["pass_rate"] == 0.75


def test_drop_manda_para_quarentena_com_motivo_e_null_conta_como_falha(df):
    rules = [
        Expectation("repo_id_not_null", "repo_id IS NOT NULL", "drop"),
        Expectation("repo_id_positivo", "repo_id > 0", "drop"),  # NULL > 0 é NULL → falha
        Expectation("tipo_conhecido", "event_type LIKE '%Event'", "warn"),
    ]
    r = apply_expectations(df, rules)
    assert {x.event_id for x in r.valid.collect()} == {1, 3, None}
    q = r.quarantine.collect()
    assert len(q) == 1 and q[0].event_id == 2
    assert q[0]._dq_failed_rules == ["repo_id_not_null", "repo_id_positivo"]
    assert "_dq_checked_at" in r.quarantine.columns and "_dq_failed_rules" not in r.valid.columns
    by_rule = {m["rule"]: m["failed"] for m in r.metrics}
    assert by_rule == {"repo_id_not_null": 1, "repo_id_positivo": 1, "tipo_conhecido": 1}


def test_fail_aborta_o_lote(df):
    with pytest.raises(ExpectationFailed) as exc:
        apply_expectations(df, [Expectation("event_id_not_null", "event_id IS NOT NULL", "fail")])
    assert exc.value.failures == {"event_id_not_null": 1}


def test_nomes_repetidos_sao_rejeitados(df):
    with pytest.raises(ValueError):
        apply_expectations(df, [Expectation("a", "true"), Expectation("a", "true")])


def test_contrato_carrega_e_regras_viram_expectations():
    c = load_contract(CONTRACT)
    assert c.primary_key == ["event_id"] and c.freshness and c.freshness.max_delay_hours == 3
    rules = expectations_from_dicts(c.expectations)
    assert {r.action for r in rules} == {"fail", "drop", "warn"}


def test_validate_schema_aponta_ausente_tipo_e_extra(spark):
    c = load_contract(CONTRACT)
    good = spark.createDataFrame([], ", ".join(f"`{col.name}` {col.type}" for col in c.columns))
    assert validate_schema(good.schema, c) == []
    bad = good.drop("repo_name").withColumn("event_id", F.col("event_id").cast("string")).withColumn(
        "novo", F.lit(1)
    )
    problems = validate_schema(bad.schema, c)
    assert "coluna ausente: repo_name" in problems
    assert "tipo diferente em event_id: contrato=bigint real=string" in problems
    assert "coluna fora do contrato: novo" in problems
    with pytest.raises(ContractViolation):
        enforce_contract(bad, c)


def test_validate_data_chave_duplicada_e_nulos(spark):
    c = load_contract(CONTRACT)
    cols = ", ".join(f"`{col.name}` {col.type}" for col in c.columns)
    base = spark.createDataFrame([], cols)
    row = {col.name: None for col in c.columns}
    rows = [dict(row, event_id=1), dict(row, event_id=1)]
    df = spark.createDataFrame(rows, base.schema)
    problems = validate_data(df, c)
    assert any("não é única" in p for p in problems)
    assert any(p.startswith("event_type tem 2 nulos") for p in problems)


def test_freshness(spark):
    # Literal SQL = fuso da sessão (UTC). `now` sem fuso = UTC. O teste passa em máquina de qualquer fuso.
    df = spark.sql("SELECT TIMESTAMP'2026-10-01 14:59:00' AS created_at")
    ok = check_freshness(df, "created_at", 3, now=datetime(2026, 10, 1, 16))
    late = check_freshness(df, "created_at", 3, now=datetime(2026, 10, 2, 0))
    assert ok.ok and ok.lag_hours == pytest.approx(1.02, abs=0.01)
    assert ok.latest == datetime(2026, 10, 1, 14, 59)
    assert not late.ok
    brt = timezone(timedelta(hours=-3))
    com_fuso = check_freshness(df, "created_at", 3, now=datetime(2026, 10, 1, 13, tzinfo=brt))
    assert com_fuso.lag_hours == ok.lag_hours  # 13h em UTC-3 = 16h UTC
    vazio = check_freshness(df.filter("false"), "created_at", 3, now=datetime(2026, 10, 1, 16))
    assert not vazio.ok and vazio.latest is None


def test_volume_anomalies_leave_one_out():
    counts = {f"h{i}": 100 + (i % 3) for i in range(10)} | {"h10": 20}
    out = {r["key"]: r for r in volume_anomalies(counts, z_threshold=3)}
    assert out["h10"]["anomaly"] and not out["h0"]["anomaly"]
    with pytest.raises(ValueError):
        volume_anomalies({"a": 1, "b": 2})
