"""Ponta a ponta sobre a amostra real: landing → bronze → silver → gold → quality, duas vezes."""

from __future__ import annotations

import shutil
from pathlib import Path

from oss_lakehouse import pipeline
from oss_lakehouse.config import get_settings


def _run_all(spark) -> dict[str, object]:
    return {
        "bronze": pipeline.run_bronze(spark),
        "silver": pipeline.run_silver(spark),
        "gold": pipeline.run_gold(spark),
        "quarantine": pipeline.run_quality(spark)["quarantine"],
    }


def test_pipeline_ponta_a_ponta_e_idempotente(spark, gh_sample_dir):
    landing = Path(get_settings().path("landing", "gharchive"))
    landing.mkdir(parents=True, exist_ok=True)
    for f in gh_sample_dir.glob("*.json.gz"):
        shutil.copy2(f, landing / f.name)

    first = _run_all(spark)
    assert first["bronze"] == 2000
    # A silver deduplica por event_id: nunca tem mais linhas que a bronze.
    assert 0 < first["silver"]["gh_events"] <= 2000
    # O fato transacional preserva o grão da silver (join point-in-time não multiplica linhas).
    assert first["gold"]["fct_events"] == first["silver"]["gh_events"]
    # A dimensão tem as versões da SCD2 + o membro "desconhecido".
    assert first["gold"]["dim_repo"] == first["silver"]["dim_repo_scd2"] + 1

    assert _run_all(spark) == first  # reprocessar não muda nada
