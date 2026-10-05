from __future__ import annotations

import gzip
import shutil
import time
from datetime import UTC, datetime, timedelta

import pytest

from oss_lakehouse.observability import (
    FAILED,
    SUCCESS,
    DeltaRunSink,
    MemorySink,
    ProgressCollector,
    check_freshness,
    check_volume,
    estimate_cost,
    last_commit_metrics,
    new_run_id,
    raise_alerts,
    track_step,
    tracked,
)


def test_track_step_registra_sucesso_com_linhas_e_duracao():
    sink = MemorySink()
    times = iter([datetime(2026, 10, 1, 12, tzinfo=UTC), datetime(2026, 10, 1, 12, 0, 3, tzinfo=UTC)])
    with track_step("p", "s", sink, clock=lambda: next(times)) as run:
        run.rows_read, run.rows_written = 10, 4
    (r,) = sink.runs
    assert (r.status, r.rows_read, r.rows_written, r.duration_s, r.error) == (SUCCESS, 10, 4, 3.0, None)
    assert r.job_group.startswith("p.s.")


def test_track_step_registra_falha_e_propaga_a_excecao():
    sink = MemorySink()
    with pytest.raises(ZeroDivisionError), track_step("p", "quebra", sink):
        1 / 0  # noqa: B018
    (r,) = sink.runs
    assert r.status == FAILED
    assert r.error.startswith("ZeroDivisionError")
    assert r.finished_at is not None


def test_falha_no_sink_nao_esconde_o_erro_da_etapa():
    class BrokenSink:
        def write(self, run):
            raise OSError("disco cheio")

    with pytest.raises(KeyError), track_step("p", "s", BrokenSink()):
        raise KeyError("erro original")


def test_tracked_le_contagens_do_retorno():
    sink = MemorySink()

    @tracked("p", "dec", sink)
    def etapa():
        return {"rows_read": 7, "rows_written": 2}

    assert etapa() == {"rows_read": 7, "rows_written": 2}
    assert (sink.runs[0].rows_read, sink.runs[0].rows_written) == (7, 2)


def test_etapas_da_mesma_execucao_compartilham_o_run_id():
    sink = MemorySink()
    rid = new_run_id()
    with track_step("p", "a", sink, run_id=rid):
        pass

    @tracked("p", "b", sink, run_id=rid)
    def etapa_b():
        return None

    etapa_b()
    with track_step("p", "outra_execucao", sink):
        pass
    assert [r.run_id == rid for r in sink.runs] == [True, True, False]
    assert sink.runs[0].job_group == f"p.a.{rid[:8]}"


def test_freshness():
    now = datetime(2026, 10, 5, 12, tzinfo=UTC)
    assert check_freshness(now - timedelta(hours=1), now, timedelta(hours=2)).ok
    late = check_freshness(now - timedelta(hours=5), now, timedelta(hours=2))
    assert not late.ok and "5.0 h" in late.observed


def test_volume_usa_mediana_e_tolerancia():
    hist = [100, 110, 90, 1000]  # o 1000 (anômalo) não mexe na mediana
    assert check_volume(105, hist, tolerance=0.2).ok
    assert not check_volume(30, hist, tolerance=0.2).ok
    assert check_volume(5, [], tolerance=0.2).ok  # primeira carga


def test_raise_alerts_notifica_so_as_falhas():
    msgs: list[str] = []
    now = datetime(2026, 10, 5, tzinfo=UTC)
    failed = raise_alerts(
        [check_volume(100, [100]), check_freshness(now - timedelta(days=1), now, timedelta(hours=1))],
        notify=msgs.append,
    )
    assert len(failed) == 1 and len(msgs) == 1 and "ALERTA" in msgs[0]


def test_estimate_cost():
    c = estimate_cost(hours=2, nodes=3, dbu_per_node_hour=1.5, dbu_price=0.30, vm_price_per_node_hour=0.5)
    assert c.dbus == pytest.approx(9.0)
    assert c.dbu_cost == pytest.approx(2.7) and c.vm_cost == pytest.approx(3.0)
    assert c.total == pytest.approx(5.7)
    with pytest.raises(ValueError):
        estimate_cost(1, 0, 1, 1)


def test_delta_run_sink_grava_sucesso_e_falha(spark, tmp_path):
    sink = DeltaRunSink(spark, str(tmp_path / "pipeline_runs"))
    with track_step("p", "ok", sink, spark) as run:
        run.rows_read = spark.range(50).count()
    with pytest.raises(ValueError), track_step("p", "falha", sink, spark):
        raise ValueError("ruim")
    rows = {r["step"]: r for r in sink.read().collect()}
    assert rows["ok"]["status"] == SUCCESS and rows["ok"]["rows_read"] == 50
    assert rows["falha"]["status"] == FAILED and "ValueError" in rows["falha"]["error"]


def test_progress_collector_captura_micro_lotes(spark, tmp_path, gh_sample_dir):
    src = tmp_path / "src"
    src.mkdir()
    for f in sorted(gh_sample_dir.glob("*.json.gz"))[:1]:
        shutil.copy(f, src / f.name)
    with gzip.open(next(src.glob("*.json.gz")), "rt") as fh:
        expected = sum(1 for _ in fh)

    collector = ProgressCollector()
    spark.streams.addListener(collector)
    try:
        q = (
            spark.readStream.schema("id STRING, type STRING")
            .json(str(src))
            .writeStream.format("delta")
            .option("checkpointLocation", str(tmp_path / "ckpt"))
            .trigger(availableNow=True)
            .queryName("teste_listener")
            .start(str(tmp_path / "out"))
        )
        q.awaitTermination()
        for _ in range(40):  # o listener é chamado de forma assíncrona
            if collector.progress:
                break
            time.sleep(0.25)
    finally:
        spark.streams.removeListener(collector)
    assert sum(p["num_input_rows"] for p in collector.progress) == expected
    assert collector.progress[0]["query"] == "teste_listener"


def test_delta_run_sink_grava_o_instante_em_utc_mesmo_com_maquina_em_outro_fuso(spark, tmp_path, monkeypatch):
    # datetime sem fuso é lido pelo PySpark como hora local: em -03:00 o instante sairia 3 h errado.
    monkeypatch.setenv("TZ", "America/Sao_Paulo")
    time.tzset()
    try:
        sink = DeltaRunSink(spark, str(tmp_path / "runs_tz"))
        times = iter([datetime(2026, 10, 1, 12, tzinfo=UTC), datetime(2026, 10, 1, 12, 0, 2, tzinfo=UTC)])
        with track_step("p", "s", sink, clock=lambda: next(times)):
            pass
        got = sink.read().selectExpr("date_format(started_at, 'yyyy-MM-dd HH:mm:ss') AS t").first()["t"]
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()
    assert got == "2026-10-01 12:00:00"  # a sessão de teste usa spark.sql.session.timeZone=UTC


def test_last_commit_metrics_devolve_as_linhas_gravadas_sem_count(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(37).write.format("delta").save(path)
    metrics = last_commit_metrics(spark, path)
    assert metrics["operation"] == "WRITE" and int(metrics["numOutputRows"]) == 37
