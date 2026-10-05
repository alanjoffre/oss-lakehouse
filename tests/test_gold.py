from __future__ import annotations

from datetime import date, datetime

from delta.tables import DeltaTable

from oss_lakehouse.gold import (
    UNKNOWN_SK,
    build_dim_actor,
    build_dim_date,
    build_dim_repo,
    build_fct_events,
    build_fct_repo_activity_daily,
    write_table,
)
from oss_lakehouse.scd2 import apply_scd2

SILVER_COLS = (
    "event_id long, event_type string, action string, created_at timestamp, event_date date, event_hour int, "
    "actor_id long, actor_login string, is_bot boolean, repo_id long, repo_name string, repo_owner string, "
    "pr_number int, issue_number int, review_state string"
)


def _silver(spark):
    d = date(2026, 10, 1)
    rows = [
        (1, "PushEvent", None, datetime(2026, 10, 1, 10), d, 10, 7, "ana", False, 100, "ana/velho", "ana"),
        (2, "PullRequestEvent", "opened", datetime(2026, 10, 1, 11), d, 11, 8, "ci[bot]", True, 100,
         "ana/velho", "ana"),
        (3, "PullRequestEvent", "merged", datetime(2026, 10, 1, 13), d, 13, 7, "ana", False, 100,
         "ana/novo", "ana"),
        (4, "WatchEvent", "started", datetime(2026, 10, 1, 14), d, 14, 9, "bia", False, 200, "bia/z", "bia"),
    ]
    return spark.createDataFrame([(*r, None, None, None) for r in rows], SILVER_COLS)


def test_dim_date_chave_e_dia_da_semana(spark):
    dd = build_dim_date(spark, date(2026, 10, 1), date(2026, 10, 4)).orderBy("date_key").collect()
    assert [r.date_key for r in dd] == [20261001, 20261002, 20261003, 20261004]
    assert dd[0].day_name == "quinta" and dd[0].month_name == "out"  # 01/10/2026 é quinta-feira
    assert [r.is_weekend for r in dd] == [False, False, True, True]


def test_fct_events_join_point_in_time_e_grao_preservado(spark, tmp_path):
    silver = _silver(spark)
    scd = str(tmp_path / "scd")
    apply_scd2(spark, silver.select("repo_id", "repo_name", "repo_owner", "created_at"), scd,
               ["repo_id"], ["repo_name", "repo_owner"], "created_at")
    dim_repo = build_dim_repo(spark, spark.read.format("delta").load(scd))
    assert dim_repo.filter(f"repo_sk = {UNKNOWN_SK}").count() == 1
    fct = build_fct_events(silver, dim_repo, build_dim_actor(silver))
    assert fct.count() == silver.count()  # o join não multiplica linhas (grão = evento)
    names = {
        r.event_id: r.repo_name
        for r in fct.join(dim_repo, "repo_sk").select("event_id", "repo_name").collect()
    }
    assert names == {1: "ana/velho", 2: "ana/velho", 3: "ana/novo", 4: "bia/z"}


def test_fct_events_sem_versao_vai_para_o_membro_desconhecido(spark, tmp_path):
    silver = _silver(spark)
    dim_repo = build_dim_repo(spark, spark.createDataFrame(
        [], "sk long, repo_id long, repo_name string, repo_owner string, valid_from timestamp, "
            "valid_to timestamp, is_current boolean"))
    fct = build_fct_events(silver, dim_repo, build_dim_actor(silver))
    assert {r.repo_sk for r in fct.collect()} == {UNKNOWN_SK}


def test_dim_actor_scd1_e_bot(spark):
    a = {r.actor_id: r for r in build_dim_actor(_silver(spark)).collect()}
    assert a[8].is_bot and not a[7].is_bot
    assert a[7].first_seen_at == datetime(2026, 10, 1, 10) and a[7].last_seen_at == datetime(2026, 10, 1, 13)


def test_fct_repo_activity_daily(spark):
    rows = {r.repo_id: r for r in build_fct_repo_activity_daily(_silver(spark)).collect()}
    r100 = rows[100]
    got = (r100.date_key, r100.events, r100.pushes, r100.prs_opened, r100.prs_merged)
    assert got == (20261001, 3, 1, 1, 1)
    assert (r100.bot_events, r100.distinct_actors) == (1, 2)
    assert rows[200].stars == 1


def test_write_table_com_liquid_clustering(spark, tmp_path):
    path = str(tmp_path / "fct")
    write_table(spark, _silver(spark), path, cluster_by=["event_date", "repo_id"])
    detail = DeltaTable.forPath(spark, path).detail().collect()[0]
    assert list(detail["clusteringColumns"]) == ["event_date", "repo_id"]
    write_table(spark, _silver(spark), path, cluster_by=["event_date", "repo_id"])  # substituir é idempotente
    assert spark.read.format("delta").load(path).count() == 4
