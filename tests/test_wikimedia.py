"""Consumidor SSE da Wikimedia sem rede: parser, micro-lotes e retomada por Last-Event-ID."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import requests

from oss_lakehouse.sources.wikimedia import (
    MicroBatchWriter,
    capture,
    load_last_event_id,
    parse_sse,
    slim_event,
)

FIXTURE = Path(__file__).parent / "fixtures" / "wikimedia" / "recentchange"


def read_gz(path: Path) -> list[dict]:
    with gzip.open(path, "rt") as f:
        return [json.loads(x) for x in f]


def _ev(i: int) -> dict:
    return {"meta": {"id": f"m{i}", "dt": f"2026-10-05T13:00:{i:02d}Z", "domain": "en.wikipedia.org"},
            "id": i, "type": "edit", "wiki": "enwiki", "bot": i % 2 == 0, "title": f"T{i}",
            "length": {"old": 1, "new": 2}, "extra": "descartado"}


def _sse_lines(events: list[dict]) -> list[str]:
    lines = [":ok", ""]
    for e in events:
        lines += ["event: message", f"id: [{{\"offset\":{e['id']}}}]", f"data: {json.dumps(e)}", ""]
    return lines


def test_parse_sse_comentario_multilinha_e_despacho():
    lines = [":ok", "", "event: message", "id: 1", "data: {\"a\":", "data: 1}", "", "data: x"]
    evs = list(parse_sse(lines))
    assert evs[0].id == "1" and json.loads(evs[0].data) == {"a": 1}
    assert evs[1].data == "x" and evs[1].event == "message"


def test_slim_event_mantem_forma_e_descarta_extra():
    s = slim_event(_ev(3))
    assert s["meta"]["id"] == "m3" and s["length"] == {"old": 1, "new": 2} and "extra" not in s


def test_microbatch_fecha_por_tamanho_e_escreve_atomico(tmp_path):
    w = MicroBatchWriter(tmp_path, max_events=2, max_seconds=999)
    assert w.add({"a": 1}) is None
    path = w.add({"a": 2})
    assert path and path.name.endswith(".jsonl.gz")
    assert read_gz(path) == [{"a": 1}, {"a": 2}]
    assert not list(tmp_path.glob("*.tmp"))


def test_microbatch_fecha_por_tempo(tmp_path):
    t = [0.0]
    w = MicroBatchWriter(tmp_path, max_events=999, max_seconds=10, clock=lambda: t[0])
    w.add({"a": 1})
    t[0] = 11.0
    assert w.add({"a": 2}) is not None


class FakeStream:
    def __init__(self, lines, fail_after: int | None = None):
        self.lines, self.fail_after = lines, fail_after

    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode=True):
        for i, line in enumerate(self.lines):
            if self.fail_after is not None and i >= self.fail_after:
                raise requests.exceptions.ChunkedEncodingError("conexão caiu")
            yield line

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeSession:
    def __init__(self, streams):
        self.streams, self.headers_seen = list(streams), []

    def get(self, url, headers, stream, timeout):
        self.headers_seen.append(dict(headers))
        return self.streams.pop(0)


def test_capture_grava_lotes_salva_estado_e_retoma(tmp_path):
    evs = [_ev(i) for i in range(5)]
    sess = FakeSession([FakeStream(_sse_lines(evs))])
    state = tmp_path / "state.json"
    rep = capture(tmp_path / "out", state, max_seconds=999, max_events=5, batch_events=2, session=sess)
    assert rep.events == 5 and len(rep.files) == 3
    assert "User-Agent" in sess.headers_seen[0] and "Last-Event-ID" not in sess.headers_seen[0]
    assert load_last_event_id(state) == '[{"offset":4}]'
    rows = [r for f in sorted((tmp_path / "out").glob("*.gz")) for r in read_gz(f)]
    assert [r["meta"]["id"] for r in rows] == [f"m{i}" for i in range(5)]

    sess2 = FakeSession([FakeStream(_sse_lines([_ev(5)]))])
    rep2 = capture(tmp_path / "out", state, max_seconds=999, max_events=1, session=sess2)
    assert rep2.resumed_from == '[{"offset":4}]'
    assert sess2.headers_seen[0]["Last-Event-ID"] == '[{"offset":4}]'


def test_capture_reconecta_com_last_event_id(tmp_path):
    evs = [_ev(i) for i in range(4)]
    lines = _sse_lines(evs)
    # cai depois do 2º evento (2 linhas de cabeçalho + 4 por evento)
    sess = FakeSession([FakeStream(lines, fail_after=10), FakeStream(_sse_lines(evs[2:]))])
    rep = capture(tmp_path / "out", tmp_path / "st.json", max_seconds=999, max_events=4,
                  batch_events=100, session=sess, sleep=lambda _: None)
    assert rep.reconnects == 1 and rep.events == 4
    assert sess.headers_seen[1]["Last-Event-ID"] == '[{"offset":1}]'


def test_capture_ignora_canary_e_json_invalido(tmp_path):
    canary = _ev(1) | {"meta": {"id": "c", "dt": "x", "domain": "canary"}}
    lines = _sse_lines([canary, _ev(2)]) + ["data: {quebrado", ""]
    rep = capture(tmp_path / "o", tmp_path / "s.json", max_seconds=999, max_events=1,
                  session=FakeSession([FakeStream(lines)]))
    assert rep.events == 1


def test_fixture_gravada_tem_formato_esperado():
    files = sorted(FIXTURE.glob("*.jsonl.gz"))
    assert len(files) >= 10
    first = read_gz(files[0])[0]
    assert {"meta", "wiki", "bot", "type", "timestamp"} <= set(first)


def test_estado_usa_temporario_com_sufixo_acrescentado(tmp_path):
    # `state.json` e `state.txt` na mesma pasta não podem disputar o mesmo temporário.
    from oss_lakehouse.sources.wikimedia import load_last_event_id, save_last_event_id

    a, b = tmp_path / "state.json", tmp_path / "state.txt"
    save_last_event_id(a, "id-a")
    save_last_event_id(b, "id-b")
    assert load_last_event_id(a) == "id-a" and load_last_event_id(b) == "id-b"
    assert not list(tmp_path.glob("*.tmp"))
