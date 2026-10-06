"""Fonte Wikimedia EventStreams: edições de todas as wikis em tempo real, via SSE.

SSE (*Server-Sent Events*) é HTTP comum com a resposta que nunca termina: o servidor manda blocos
`event:`/`id:`/`data:` separados por linha em branco. Cada evento traz um `id` (aqui, a posição no
Kafka por trás do serviço); quem reconecta mandando `Last-Event-ID` retoma de onde parou.

Este consumidor grava **micro-lotes JSONL.gz** na landing (escrita atômica: `.tmp` → rename) e
persiste o último `id` só DEPOIS de o lote estar em disco: ao cair, reprocessa no máximo um lote
(at-least-once) — a deduplicação por `meta.id` na Silver fecha a conta.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# A Wikimedia bloqueia cliente sem User-Agent descritivo (política de UA da fundação).
USER_AGENT = (
    "oss-lakehouse-study/0.1 (https://github.com/alanjoffre/oss-lakehouse; estudo de engenharia de dados)"
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSEEvent:
    """Um evento SSE já montado: tipo (`message` por padrão), `id` (se veio) e as linhas `data:` unidas."""

    event: str
    id: str | None
    data: str


def parse_sse(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Converte linhas de um stream SSE em eventos (especificação WHATWG, o essencial).

    - `:` no início = comentário/keep-alive (ignorado);
    - várias linhas `data:` no mesmo evento são unidas por `\\n`;
    - linha em branco despacha o evento.
    """
    event, eid, data = "message", None, []
    for raw in lines:
        line = raw.rstrip("\r")
        if not line:
            if data:
                yield SSEEvent(event, eid, "\n".join(data))
            event, eid, data = "message", None, []
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if name == "data":
            data.append(value)
        elif name == "id":
            eid = value
        elif name == "event":
            event = value
    if data:
        yield SSEEvent(event, eid, "\n".join(data))


def slim_event(ev: dict[str, Any]) -> dict[str, Any]:
    """Recorta o evento ao que o pipeline usa — mantém a forma aninhada original."""
    meta = ev.get("meta", {})
    return {
        "meta": {k: meta.get(k) for k in ("id", "dt", "domain", "partition", "offset")},
        "id": ev.get("id"),
        "type": ev.get("type"),
        "namespace": ev.get("namespace"),
        "title": ev.get("title"),
        "user": ev.get("user"),
        "bot": ev.get("bot"),
        "minor": ev.get("minor"),
        "timestamp": ev.get("timestamp"),
        "wiki": ev.get("wiki"),
        "server_name": ev.get("server_name"),
        "length": ev.get("length"),
        "log_type": ev.get("log_type"),
    }


@dataclass
class MicroBatchWriter:
    """Acumula eventos e grava um arquivo por lote quando enche (`max_events`) ou vence (`max_seconds`)."""

    out_dir: Path
    max_events: int = 1000
    max_seconds: float = 20.0
    clock: Callable[[], float] = time.monotonic
    buffer: list[dict[str, Any]] = field(default_factory=list)
    files: list[Path] = field(default_factory=list)
    _opened_at: float | None = None

    def add(self, ev: dict[str, Any]) -> Path | None:
        if self._opened_at is None:
            self._opened_at = self.clock()
        self.buffer.append(ev)
        if len(self.buffer) >= self.max_events or self.clock() - self._opened_at >= self.max_seconds:
            return self.flush()
        return None

    def flush(self) -> Path | None:
        if not self.buffer:
            return None
        self.out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        path = self.out_dir / f"rc-{stamp}-{len(self.files):05d}.jsonl.gz"
        tmp = path.with_name(path.name + ".tmp")  # `.tmp` não casa com o glob do leitor
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            for ev in self.buffer:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        os.replace(tmp, path)
        self.files.append(path)
        self.buffer, self._opened_at = [], None
        return path


def load_last_event_id(state_path: Path) -> str | None:
    """`Last-Event-ID` salvo no arquivo de estado; `None` se o arquivo não existe (primeira execução)."""
    return json.loads(state_path.read_text()).get("last_event_id") if state_path.exists() else None


def save_last_event_id(state_path: Path, last_id: str) -> None:
    """Grava o estado de retomada de forma atômica (`.tmp` + `os.replace`), com o instante em UTC."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    # Sufixo ACRESCENTADO (state.json.tmp), não trocado: dois estados com o mesmo nome-base não colidem.
    tmp = state_path.with_name(state_path.name + ".tmp")
    tmp.write_text(json.dumps({"last_event_id": last_id, "saved_at": datetime.now(UTC).isoformat()}))
    os.replace(tmp, state_path)


@dataclass
class CaptureReport:
    """Resumo de uma captura: eventos lidos, arquivos gravados, reconexões e os `Last-Event-ID`
    (de onde retomou e o último já salvo no estado).
    """

    events: int = 0
    files: list[Path] = field(default_factory=list)
    reconnects: int = 0
    resumed_from: str | None = None
    last_event_id: str | None = None


def capture(
    out_dir: str | Path,
    state_path: str | Path,
    max_seconds: float = 60.0,
    max_events: int | None = None,
    batch_events: int = 1000,
    batch_seconds: float = 20.0,
    session: requests.Session | None = None,
    url: str = STREAM_URL,
    slim: bool = True,
    max_reconnects: int = 5,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> CaptureReport:
    """Consome o stream por `max_seconds` (ou `max_events`) gravando micro-lotes na landing.

    Retoma pelo `Last-Event-ID` salvo em `state_path`; reconecta com backoff se a conexão cair.
    """
    out, state = Path(out_dir), Path(state_path)
    session = session or requests.Session()
    writer = MicroBatchWriter(out, batch_events, batch_seconds, clock=clock)
    rep = CaptureReport(resumed_from=load_last_event_id(state))
    last_id, pending_id = rep.resumed_from, None
    deadline = clock() + max_seconds
    attempt = 0

    def done() -> bool:
        return clock() >= deadline or (max_events is not None and rep.events >= max_events)

    while not done():
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            with session.get(url, headers=headers, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                attempt = 0
                for ev in parse_sse(r.iter_lines(decode_unicode=True)):
                    if ev.event != "message":
                        continue
                    try:
                        payload = json.loads(ev.data)
                    except json.JSONDecodeError:
                        log.warning("evento com JSON inválido ignorado")
                        continue
                    if payload.get("meta", {}).get("domain") == "canary":
                        continue  # evento sintético de monitoramento da Wikimedia
                    pending_id = ev.id or pending_id
                    rep.events += 1
                    if writer.add(slim_event(payload) if slim else payload):
                        _commit(writer, rep, state, pending_id)
                    if done():
                        break
                last_id = pending_id or last_id
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
            attempt += 1
            rep.reconnects += 1
            if attempt > max_reconnects:
                raise
            last_id = pending_id or last_id
            wait = min(30.0, 2.0 ** attempt)
            log.warning("stream caiu (%s); reconectando em %.0fs com Last-Event-ID", exc, wait)
            sleep(wait)
    if writer.flush():
        _commit(writer, rep, state, pending_id)
    return rep


def _commit(writer: MicroBatchWriter, rep: CaptureReport, state: Path, last_id: str | None) -> None:
    """Depois que o arquivo está em disco: registra o lote e avança o estado (dado → estado)."""
    rep.files.append(writer.files[-1])
    if last_id:
        save_last_event_id(state, last_id)
        rep.last_event_id = last_id
