from __future__ import annotations

import itertools
import time

import pytest

from oss_lakehouse.perf import (
    SparkUI,
    as_table,
    compare,
    delta_active_files,
    dist_stats,
    files_read,
    grep_plan,
    job_group,
    parse_metric_value,
    plan_text,
    salted_join,
    spark_conf,
    stopwatch,
    time_it,
)


def _fake_clock(step: float = 1.0):
    counter = itertools.count()
    return lambda: next(counter) * step


def test_time_it_mede_repeat_vezes_e_ignora_o_aquecimento():
    calls = []
    t = time_it(lambda: calls.append(1), repeat=3, warmup=2, label="x", clock=_fake_clock(0.5))
    assert len(calls) == 5  # 2 de aquecimento + 3 medidas
    assert t.runs == (0.5, 0.5, 0.5)
    assert t.median == 0.5 and t.best == 0.5
    assert "mediana" in str(t)


def test_time_it_rejeita_repeat_zero():
    with pytest.raises(ValueError):
        time_it(lambda: None, repeat=0)


def test_compare_mostra_razao_em_relacao_a_mais_rapida():
    from oss_lakehouse.perf import Timing

    out = compare([Timing("rápida", (1.0, 1.0, 1.0)), Timing("lenta", (3.0, 3.0, 3.0))])
    assert "1.0x" in out.splitlines()[0] and "3.0x" in out.splitlines()[1]
    assert compare([]) == ""


def test_stopwatch_preenche_segundos_mesmo_com_erro():
    with pytest.raises(RuntimeError), stopwatch(clock=_fake_clock(2.0)) as sw:
        raise RuntimeError("x")
    assert sw.seconds == 2.0


def test_dist_stats_detecta_skew():
    balanced = dist_stats([100, 110, 90, 105])
    skewed = dist_stats([100] * 19 + [5000])
    assert balanced.max_over_median < 1.2
    assert skewed.max_over_median == 50
    assert skewed.p95 == 100 and skewed.max == 5000
    with pytest.raises(ValueError):
        dist_stats([])


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("12", 12),
        ("1,234", 1234),
        ("10.0 MiB", 10 * 1024**2),
        ("total (min, med, max (stageId: taskId))\n3.0 s (1.0 s, 1.0 s, 1.0 s (stage 1.0: task 2))", 3000),
        ("", None),
    ],
)
def test_parse_metric_value(text, expected):
    assert parse_metric_value(text) == expected


def test_sparkui_le_stages_tasks_e_scan_de_um_job_group(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(10_000).selectExpr("id", "id % 7 AS k").repartition(3).write.format("delta").save(path)
    ui = SparkUI.of(spark)
    with job_group(spark, "teste-perf"):
        spark.read.format("delta").load(path).groupBy("k").count().collect()

    summaries = ui.stage_summaries("teste-perf")
    assert summaries, "deveria achar os stages do grupo"
    assert sum(s.num_tasks for s in summaries) >= 2
    heavy = ui.heaviest_shuffle_stage("teste-perf")
    recs = ui.task_shuffle_records(heavy["stageId"], heavy["attemptId"])
    assert recs.n >= 1
    assert files_read(ui, "teste-perf") == 3
    # depois do bloco o grupo é limpo: um job novo não herda o rótulo
    with job_group(spark, "depois"):
        spark.range(10).count()
    assert ui.jobs("depois")  # espera o status store da UI registrar o job novo
    spark.range(10).count()
    time.sleep(1)
    latest = max(ui.get("jobs"), key=lambda j: j["jobId"])
    assert latest.get("jobGroup") is None


def test_grep_plan_acha_o_exchange_de_uma_agregacao(spark):
    df = spark.range(100).selectExpr("id % 3 AS k").groupBy("k").count()
    assert any("Exchange" in line for line in grep_plan(df, "Exchange"))
    assert "== Physical Plan ==" in plan_text(df)


def test_as_table_alinha_e_trunca():
    out = as_table([{"a": 1, "b": "x"}, {"a": 1000, "b": "yy"}], max_rows=1)
    lines = out.splitlines()
    assert lines[0].split() == ["a", "b"]
    assert "+1 linhas" in lines[-1]
    assert as_table([]) == "(vazio)"


def test_delta_active_files_faz_log_replay_com_stats(spark, tmp_path):
    path = str(tmp_path / "skip")
    spark.range(0, 100, numPartitions=2).write.format("delta").save(path)
    spark.range(100, 150, numPartitions=1).write.format("delta").mode("append").save(path)
    files = delta_active_files(path)
    assert len(files) == 3
    assert sum(f["num_records"] for f in files) == 150
    assert min(f["min"]["id"] for f in files) == 0 and max(f["max"]["id"] for f in files) == 149
    spark.range(0, 5, numPartitions=1).write.format("delta").mode("overwrite").save(path)  # remove 3, add 1
    assert [f["num_records"] for f in delta_active_files(path)] == [5]


def test_salted_join_da_o_mesmo_resultado_que_o_join_comum(spark):
    big = spark.createDataFrame([("bot", i) for i in range(50)] + [("ana", 1), ("bia", 2)], "k string, v int")
    small = spark.createDataFrame([("bot", "B"), ("ana", "A"), ("zed", "Z")], "k string, label string")
    plain = sorted(big.join(small, "k").collect())
    salted = salted_join(big, small, "k", hot_keys=["bot"], buckets=4)
    assert sorted(salted.select("k", "v", "label").collect()) == plain
    assert "_salt" not in salted.columns
    with pytest.raises(ValueError):
        salted_join(big, small, "k", ["bot"], buckets=1)


def test_spark_conf_restaura_o_valor_anterior(spark):
    key = "spark.sql.shuffle.partitions"
    before = spark.conf.get(key)
    with spark_conf(spark, {key: "123", "spark.sql.adaptive.enabled": "false"}):
        assert spark.conf.get(key) == "123"
    assert spark.conf.get(key) == before
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
