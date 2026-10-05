from __future__ import annotations

from datetime import datetime

import pytest

from oss_lakehouse.bronze import ingest_gharchive_bronze
from oss_lakehouse.sources.gharchive import hour_keys
from oss_lakehouse.utils.retry import retry


def test_hour_keys_sem_zero_a_esquerda_e_inclusivo():
    keys = list(hour_keys(datetime(2026, 10, 1, 8), datetime(2026, 10, 1, 10)))
    assert keys == ["2026-10-01-8", "2026-10-01-9", "2026-10-01-10"]


def test_retry_tenta_ate_dar_certo_sem_esperar_de_verdade():
    calls, sleeps = [], []

    @retry(exceptions=(ValueError,), attempts=3, sleep=sleeps.append)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("falhou")
        return "ok"

    assert flaky() == "ok"
    assert len(calls) == 3 and len(sleeps) == 2


def test_retry_esgota_e_propaga_a_excecao():
    @retry(exceptions=(ValueError,), attempts=2, sleep=lambda _: None)
    def always_fails():
        raise ValueError("sempre")

    with pytest.raises(ValueError):
        always_fails()


def test_bronze_e_idempotente(spark, gh_sample_dir, tmp_path):
    target, ckpt = str(tmp_path / "bronze"), str(tmp_path / "ckpt")
    ingest_gharchive_bronze(spark, str(gh_sample_dir), target, ckpt)
    first = spark.read.format("delta").load(target)
    n = first.count()
    assert n == 2000
    assert {"_source_file", "_ingested_at", "event_date"} <= set(first.columns)

    ingest_gharchive_bronze(spark, str(gh_sample_dir), target, ckpt)  # reprocessar não duplica
    assert spark.read.format("delta").load(target).count() == n
