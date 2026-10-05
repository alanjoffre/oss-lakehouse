"""Testes de utils/io, utils/timing e utils/logs — sem Spark, rodam em milissegundos.

Mostram os 4 recursos do pytest que o notebook 02 explica: fixture, parametrize, monkeypatch e tmp_path.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
from pathlib import Path

import pytest

from oss_lakehouse.utils import io as uio
from oss_lakehouse.utils.io import (
    BadLineError,
    atomic_write,
    count_lines_gz,
    iter_jsonl_gz,
    iter_jsonl_gz_many,
)
from oss_lakehouse.utils.logs import json_logger
from oss_lakehouse.utils.timing import Timer, timed


@pytest.fixture
def jsonl_gz(tmp_path: Path) -> Path:
    """Arquivo .json.gz com 3 eventos e 1 linha corrompida (linha 3) e 1 em branco."""
    lines = [
        json.dumps({"id": "1", "type": "PushEvent"}),
        json.dumps({"id": "2", "type": "WatchEvent"}),
        '{"id": "3", "type": ',  # truncada
        "",
        json.dumps({"id": "4", "type": "PushEvent"}),
    ]
    p = tmp_path / "eventos.json.gz"
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return p


def test_generator_e_lazy(jsonl_gz):
    it = iter_jsonl_gz(jsonl_gz)
    assert next(it) == {"id": "1", "type": "PushEvent"}  # nada além da 1ª linha foi parseado


def test_linha_ruim_falha_alto_com_contexto(jsonl_gz):
    with pytest.raises(BadLineError) as exc:
        list(iter_jsonl_gz(jsonl_gz))
    assert exc.value.line_no == 3
    assert isinstance(exc.value.__cause__, json.JSONDecodeError)  # `raise ... from` preserva a causa


def test_skip_pula_e_anota_a_linha(jsonl_gz):
    bad: list[int] = []
    ids = [e["id"] for e in iter_jsonl_gz(jsonl_gz, on_error="skip", bad_lines=bad)]
    assert ids == ["1", "2", "4"]
    assert bad == [3]


@pytest.mark.parametrize(("copias", "esperado"), [(1, 3), (2, 6), (3, 9)])
def test_many_encadeia(jsonl_gz, copias, esperado):
    assert sum(1 for _ in iter_jsonl_gz_many([jsonl_gz] * copias, on_error="skip")) == esperado


def test_json_sem_gzip(tmp_path):
    p = tmp_path / "x.json"
    p.write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")
    assert [e["a"] for e in iter_jsonl_gz(p)] == [1, 2]


def test_count_by_field(jsonl_gz):
    assert uio.count_by_field(jsonl_gz) == {"PushEvent": 2, "WatchEvent": 1}


def test_count_lines_gz(jsonl_gz):
    assert count_lines_gz(jsonl_gz) == 5


def test_atomic_write_sucesso(tmp_path):
    dest = tmp_path / "out.txt"
    with atomic_write(dest) as f:
        f.write("ok")
        assert not dest.exists()  # durante a escrita o destino ainda não existe
    assert dest.read_text() == "ok"
    assert not (tmp_path / "out.txt.tmp").exists()


def test_atomic_write_erro_preserva_o_antigo(tmp_path):
    dest = tmp_path / "out.txt"
    dest.write_text("versão antiga")
    with pytest.raises(RuntimeError), atomic_write(dest) as f:
        f.write("metade")
        raise RuntimeError("caiu no meio")
    assert dest.read_text() == "versão antiga"
    assert not (tmp_path / "out.txt.tmp").exists()


def test_atomic_write_rename_falha(tmp_path, monkeypatch):
    """monkeypatch: simula falha do SO no rename sem precisar de um disco cheio de verdade."""

    def boom(src, dst):
        raise OSError("disco cheio")

    monkeypatch.setattr(uio.os, "replace", boom)
    dest = tmp_path / "out.txt"
    with pytest.raises(OSError, match="disco cheio"), atomic_write(dest) as f:
        f.write("x")
    assert not dest.exists()
    assert not (tmp_path / "out.txt.tmp").exists()


def test_timed_loga_duracao_e_status(caplog):
    @timed(level=logging.INFO)
    def soma(a: int, b: int) -> int:
        return a + b

    with caplog.at_level(logging.INFO):
        assert soma(1, 2) == 3
    rec = caplog.records[-1]
    assert rec.status == "ok" and rec.duration_s >= 0 and "soma" in rec.func
    assert soma.__name__ == "soma"  # functools.wraps preserva o nome


def test_timed_sem_parenteses_registra_erro(caplog):
    @timed
    def quebra() -> None:
        raise ValueError("x")

    with caplog.at_level(logging.INFO), pytest.raises(ValueError):
        quebra()
    assert caplog.records[-1].status == "error"


def test_timer_mede_mesmo_com_excecao(monkeypatch):
    ticks = iter([10.0, 12.5])
    monkeypatch.setattr("oss_lakehouse.utils.timing.time.perf_counter", lambda: next(ticks))
    with pytest.raises(KeyError), Timer() as t:
        raise KeyError("x")
    assert t.seconds == 2.5


def test_json_logger_campos_extra():
    buf = io.StringIO()
    log = json_logger("teste.json", stream=buf)
    json_logger("teste.json", stream=buf)  # 2ª chamada não duplica handler
    log.info("lote %s carregado", "b1", extra={"run_id": "r-42", "linhas": 10})
    lines = buf.getvalue().strip().splitlines()
    assert len(lines) == 1
    doc = json.loads(lines[0])
    assert doc["msg"] == "lote b1 carregado"
    assert doc["run_id"] == "r-42" and doc["linhas"] == 10 and doc["level"] == "INFO"
