"""Testes das soluções de live coding (notebook 16): Python puro e PySpark."""

from __future__ import annotations

import random
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from oss_lakehouse.utils.live_coding import (
    activity_streaks,
    dedup_latest,
    external_top_k,
    merge_intervals,
    sessionize,
    stable_bucket,
    top_k,
)

# --------------------------------------------------------------------------- Python puro


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [
        ([], []),
        ([(1, 3)], [(1, 3)]),
        ([(1, 3), (2, 6), (8, 10), (15, 18)], [(1, 6), (8, 10), (15, 18)]),
        ([(1, 4), (4, 5)], [(1, 5)]),  # encostar também une
        ([(8, 10), (1, 3), (2, 6)], [(1, 6), (8, 10)]),  # entrada fora de ordem
        ([(1, 10), (2, 3)], [(1, 10)]),  # contido
    ],
)
def test_merge_intervals(entrada, esperado):
    assert merge_intervals(entrada) == esperado


def test_merge_intervals_com_datas():
    d = date
    assert merge_intervals([(d(2026, 1, 5), d(2026, 1, 9)), (d(2026, 1, 1), d(2026, 1, 6))]) == [
        (d(2026, 1, 1), d(2026, 1, 9))
    ]


def test_top_k_desempata_pela_chave():
    assert top_k(["b", "a", "c", "a", "b", "d"], 2) == [("a", 2), ("b", 2)]


def test_stable_bucket_e_deterministico():
    assert stable_bucket("github-actions[bot]", 16) == stable_bucket("github-actions[bot]", 16)
    assert 0 <= stable_bucket("x", 7) < 7


def test_external_top_k_bate_com_o_em_memoria(tmp_path):
    rnd = random.Random(7)
    keys = [f"user{rnd.randint(0, 500)}" for _ in range(20_000)] + ["bot"] * 3_000
    seen: list[int] = []
    got = external_top_k(
        iter(keys), 5, tmp_path / "spill", n_partitions=8, on_partition=lambda i, n: seen.append(n)
    )
    assert got == top_k(keys, 5)
    assert got[0] == ("bot", 3_000)
    assert len(seen) == 8 and sum(seen) == len(set(keys))  # cada chave em exatamente 1 partição
    assert not list((tmp_path / "spill").iterdir())  # limpou os arquivos de spill


# --------------------------------------------------------------------------- PySpark


def test_dedup_latest_mantem_o_mais_recente_com_desempate(spark):
    df = spark.createDataFrame(
        [("a", 1, "v1"), ("a", 3, "v3"), ("a", 3, "v3b"), ("b", 2, "w")],
        "k string, ts int, v string",
    )
    out = dedup_latest(df, "k", [F.col("ts").desc(), F.col("v").desc()])
    assert sorted(out.select("k", "v").collect()) == [("a", "v3b"), ("b", "w")]


def test_sessionize_corta_acima_de_30_min(spark):
    ts = [
        ("u1", "2026-10-01 12:00:00"),
        ("u1", "2026-10-01 12:20:00"),
        ("u1", "2026-10-01 12:50:00"),  # 30 min exatos: mesma sessão (> 30 corta)
        ("u1", "2026-10-01 13:21:00"),  # 31 min: nova
        ("u2", "2026-10-01 12:00:00"),
    ]
    df = spark.createDataFrame(ts, "user string, s string").withColumn("ts", F.to_timestamp("s"))
    out = sessionize(df, "user", "ts", gap_minutes=30)
    rows = [(r.user, r.session_n) for r in out.orderBy("user", "ts").collect()]
    assert rows == [("u1", 1), ("u1", 1), ("u1", 1), ("u1", 2), ("u2", 1)]
    assert out.select("session_id").distinct().count() == 3


def test_activity_streaks_gaps_and_islands(spark):
    dias = [
        ("ana", "2026-09-01"),
        ("ana", "2026-09-02"),
        ("ana", "2026-09-02"),  # duplicado no mesmo dia
        ("ana", "2026-09-03"),
        ("ana", "2026-09-05"),
        ("bia", "2026-09-10"),
    ]
    df = spark.createDataFrame(dias, "actor string, d string")
    out = activity_streaks(df, "actor", "d").orderBy("actor", "start").collect()
    got = [(r.actor, str(r.start), str(r.end), r.days) for r in out]
    assert got == [
        ("ana", "2026-09-01", "2026-09-03", 3),
        ("ana", "2026-09-05", "2026-09-05", 1),
        ("bia", "2026-09-10", "2026-09-10", 1),
    ]


def test_sessionize_aceita_timestamp_real(spark):
    df = spark.createDataFrame(
        [("x", datetime(2026, 10, 1, 12)), ("x", datetime(2026, 10, 1, 14))], "u string, ts timestamp"
    )
    assert [r.session_n for r in sessionize(df, "u", "ts").orderBy("ts").collect()] == [1, 2]
