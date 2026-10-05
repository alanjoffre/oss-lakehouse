"""Leitura e escrita de arquivos com memória constante e sem arquivo pela metade.

- `iter_jsonl_gz`: generator que lê um JSON lines comprimido (o formato do GH Archive) linha a
  linha. A memória fica constante (~1 linha por vez), não proporcional ao arquivo.
- `iter_jsonl_gz_many`: o mesmo para vários arquivos, em sequência (lazy).
- `atomic_write`: context manager que grava em `.tmp` e renomeia no fim — quem lê nunca vê
  arquivo incompleto (mesma ideia do download do GH Archive em `sources/gharchive.py`).
"""

from __future__ import annotations

import gzip
import json
import logging
import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Literal

log = logging.getLogger(__name__)

OnError = Literal["raise", "skip"]


class BadLineError(ValueError):
    """Linha que não é JSON válido. Carrega arquivo e número da linha para o diagnóstico."""

    def __init__(self, path: str | Path, line_no: int, cause: Exception) -> None:
        super().__init__(f"{path}:{line_no}: JSON inválido ({cause})")
        self.path = str(path)
        self.line_no = line_no


def iter_jsonl_gz(
    path: str | Path,
    on_error: OnError = "raise",
    bad_lines: list[int] | None = None,
) -> Iterator[dict[str, Any]]:
    """Gera um dict por linha de um `.json.gz` (ou `.json`), sem carregar o arquivo na memória.

    on_error="raise": a 1ª linha ruim interrompe com `BadLineError` (padrão: falhar alto).
    on_error="skip": pula a linha ruim e anota o número dela em `bad_lines` (se informado) —
    é o embrião de uma quarentena (*dead-letter*): o lote segue, e o erro fica rastreável.
    Linhas em branco são ignoradas.
    """
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                if on_error == "raise":
                    raise BadLineError(path, line_no, exc) from exc
                log.warning("linha ruim ignorada em %s:%d", path, line_no)
                if bad_lines is not None:
                    bad_lines.append(line_no)


def iter_jsonl_gz_many(paths: Iterable[str | Path], on_error: OnError = "raise") -> Iterator[dict[str, Any]]:
    """Encadeia vários arquivos. Lazy: o 2º arquivo só é aberto quando o 1º acabar."""
    for p in paths:
        yield from iter_jsonl_gz(p, on_error=on_error)


def count_by_field(path: str | Path, field: str = "type") -> dict[str, int]:
    """Conta eventos por um campo de 1º nível (ex.: `type`) — parse de JSON, trabalho de CPU.

    Fica num módulo (e não no notebook) para poder ser enviado a um `ProcessPoolExecutor`:
    com `spawn` (padrão no macOS/Windows) ou `forkserver` (padrão no Linux a partir do 3.14) a função
    precisa ser importável pelo processo filho.
    """
    counts: dict[str, int] = {}
    for event in iter_jsonl_gz(path, on_error="skip"):
        key = str(event.get(field))
        counts[key] = counts.get(key, 0) + 1
    return counts


def count_lines_gz(path: str | Path) -> int:
    """Conta linhas de um `.gz` lendo em blocos binários (não decodifica nem parseia JSON)."""
    n = 0
    with gzip.open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            n += chunk.count(b"\n")
    return n


@contextmanager
def atomic_write(path: str | Path, mode: Literal["w", "wb"] = "w") -> Iterator[IO[Any]]:
    """Grava em `<path>.tmp` e só renomeia para `path` se o bloco terminar sem exceção.

    `os.replace` é atômico no mesmo sistema de arquivos (POSIX e Windows): o leitor vê o arquivo
    antigo ou o novo, nunca a metade. Em erro, o `.tmp` é apagado e a exceção sobe.
    """
    dest = Path(path)
    tmp = dest.with_name(dest.name + ".tmp")
    encoding = None if "b" in mode else "utf-8"
    f = open(tmp, mode, encoding=encoding)  # noqa: SIM115 — o fechamento é controlado abaixo
    try:
        yield f
        f.close()
        os.replace(tmp, dest)
    except BaseException:
        f.close()
        tmp.unlink(missing_ok=True)
        raise
