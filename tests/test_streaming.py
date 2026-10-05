"""Structured Streaming: janela com watermark, descarte de atrasado e foreachBatch idempotente."""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path

from pyspark.sql import functions as F

from oss_lakehouse.streaming import (
    WIKI_SCHEMA,
    edits_per_window,
    merge_batch,
    parse_wiki,
    progress_summary,
    read_wiki_stream,
)


def _write(dir_: Path, name: str, rows: list[dict], mtime: int) -> None:
    dir_.mkdir(parents=True, exist_ok=True)
    p = dir_ / name
    with gzip.open(p, "wt") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.utime(p, (mtime, mtime))  # o file source ordena por data de modificação


def _ev(eid: str, dt: str, wiki: str = "enwiki", bot: bool = False) -> dict:
    return {"meta": {"id": eid, "dt": dt}, "wiki": wiki, "bot": bot, "type": "edit",
            "length": {"old": 10, "new": 15}}


def test_parse_wiki_tempo_de_evento_e_bot(spark, tmp_path):
    _write(tmp_path / "in", "a.jsonl.gz", [_ev("1", "2026-10-05T13:00:10.5Z", bot=True)], 1_000)
    batch = spark.read.schema(WIKI_SCHEMA).json(str(tmp_path / "in")).select("*", "_metadata")
    # Formata no Spark (sessão em UTC): o collect() converteria para o fuso da máquina.
    row = parse_wiki(batch).withColumn("t", F.date_format("event_time", "HH:mm:ss.SSS")).first()
    assert row.event_id == "1" and row.is_bot is True and row.bytes_delta == 5
    assert row.t == "13:00:10.500"


def windows(spark, target: str) -> set[tuple[str, int]]:
    rows = spark.read.format("delta").load(target).select(
        F.date_format("window_start", "HH:mm").alias("w"), "edits").collect()
    return {(r.w, r.edits) for r in rows}


def test_janela_watermark_descarta_atrasado_e_merge_idempotente(spark, tmp_path):
    land, ckpt, target = tmp_path / "land", str(tmp_path / "ckpt"), str(tmp_path / "agg")
    _write(land, "1.jsonl.gz", [_ev("a", "2026-10-05T13:00:10Z"), _ev("b", "2026-10-05T13:00:50Z")], 1_000)
    _write(land, "2.jsonl.gz", [_ev("c", "2026-10-05T13:03:00Z")], 2_000)  # watermark → 13:02

    calls: list[tuple[int, int]] = []
    sink = merge_batch(target, ["window_start", "wiki", "is_bot"], on_batch=lambda b, n: calls.append((b, n)))

    def run() -> list[dict]:
        events = parse_wiki(read_wiki_stream(spark, str(land)))
        agg = edits_per_window(events, "1 minute", watermark="1 minute")
        q = (agg.writeStream.outputMode("update").foreachBatch(sink)
             .option("checkpointLocation", ckpt).trigger(availableNow=True).start())
        q.awaitTermination()
        return [p for p in q.recentProgress if p["numInputRows"] > 0]

    run()
    got = windows(spark, target)
    assert got == {("13:00", 2), ("13:03", 1)}

    # Evento de 13:00:30 chega depois de o watermark passar de 13:02 → descartado.
    _write(land, "3.jsonl.gz", [_ev("late", "2026-10-05T13:00:30Z")], 3_000)
    progress = run()
    # Exatamente 1: o sink faz persist do lote; sem isso o plano roda 2x e a métrica sai dobrada.
    assert progress and progress_summary(progress[-1])["droppedByWatermark"] == 1
    got = windows(spark, target)
    assert ("13:00", 2) in got  # a janela não mudou

    # Reaplicar o MESMO micro-lote (o que o Spark faz após falha) não duplica nem soma de novo.
    before = sorted(map(tuple, spark.read.format("delta").load(target).collect()))
    replay = spark.read.format("delta").load(target)
    sink(replay, 0)
    after = sorted(map(tuple, spark.read.format("delta").load(target).collect()))
    assert before == after
