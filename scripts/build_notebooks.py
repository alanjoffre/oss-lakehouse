"""Gera os notebooks .ipynb a partir das fontes .py (formato jupytext "percent") e os executa.

Por que fonte em .py: diff legível no Git e code review de verdade (o .ipynb é JSON com saídas).
O .ipynb executado também vai para o Git — quem abre no GitHub vê o resultado sem rodar nada.

Uso:
    uv run python scripts/build_notebooks.py            # todos
    uv run python scripts/build_notebooks.py 05 09      # só os que começam com 05 e 09
    uv run python scripts/build_notebooks.py --no-exec  # só converte
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import jupytext
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "notebooks" / "_src"
OUT = ROOT / "notebooks"


def build(src: Path, execute: bool, timeout: int) -> float:
    nb = jupytext.read(src)
    nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
    start = time.time()
    if execute:
        ExecutePreprocessor(timeout=timeout, kernel_name="python3").preprocess(
            nb, {"metadata": {"path": str(OUT)}}
        )
    for cell in nb.cells:
        if cell.cell_type == "code":
            # stderr aqui é ruído da JVM (Ivy, log4j, WARN de hostname) — não é conteúdo do notebook.
            cell.outputs = [o for o in cell.get("outputs", []) if o.get("name") != "stderr"]
    nbformat.write(nb, OUT / f"{src.stem}.ipynb")
    return time.time() - start


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("prefixes", nargs="*")
    ap.add_argument("--no-exec", action="store_true")
    ap.add_argument("--timeout", type=int, default=1800)
    args = ap.parse_args()

    sources = sorted(SRC.glob("[0-9][0-9]_*.py"))
    if args.prefixes:
        sources = [s for s in sources if any(s.name.startswith(p) for p in args.prefixes)]
    if not sources:
        print("nenhum notebook encontrado", file=sys.stderr)
        return 1
    for src in sources:
        secs = build(src, execute=not args.no_exec, timeout=args.timeout)
        print(f"ok  {src.stem}.ipynb  ({secs:.0f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
