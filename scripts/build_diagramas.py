"""Gera docs/diagramas.md com os diagramas Mermaid dos notebooks e aponta cada notebook para lá.

Por quê: o visualizador de notebooks do GitHub não renderiza Mermaid (mostra o código); páginas
Markdown renderizam. A fonte continua sendo o notebook — esta página é derivada (não editar à mão).

Uso: uv run python scripts/build_diagramas.py   (depois: build_notebooks.py --text-only)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "notebooks" / "_src"
OUT = ROOT / "docs" / "diagramas.md"
PAGE_URL = "https://github.com/alanjoffre/oss-lakehouse/blob/main/docs/diagramas.md"
POINTER = "# > 📐 O visualizador de notebooks do GitHub não renderiza Mermaid — diagrama renderizado: "
OPEN = "# ```mermaid"
CLOSE = "# ```"


def process(src: Path) -> list[tuple[str, str, str]]:
    """Extrai (âncora, título da seção, mermaid) e insere/atualiza o ponteiro acima de cada bloco."""
    lines = src.read_text(encoding="utf-8").splitlines()
    number = src.stem[:2]
    out: list[str] = []
    found: list[tuple[str, str, str]] = []
    heading = ""
    i = 0
    while i < len(lines):
        line = lines[i]
        if m := re.match(r"^# (#{1,4}) (.+)$", line):
            heading = m.group(2).strip()
        if line.strip() == OPEN:
            anchor = f"nb{number}-{len(found) + 1}"
            j = i + 1
            while j < len(lines) and lines[j].strip() != CLOSE:
                j += 1
            body = "\n".join(ln[2:] if ln.startswith("# ") else ln.lstrip("#") for ln in lines[i + 1 : j])
            found.append((anchor, heading, body))
            # Ponteiro idempotente: remove o anterior (e a linha em branco dele), se houver, e recoloca.
            while out and (out[-1].startswith(POINTER) or out[-1].strip() == "#"):
                out.pop()
            out += ["#", f"{POINTER}[docs/diagramas.md]({PAGE_URL}#{anchor})", "#"]
            out += lines[i : j + 1]
            i = j + 1
            continue
        out.append(line)
        i += 1
    new = "\n".join(out) + "\n"
    if new != src.read_text(encoding="utf-8"):
        src.write_text(new, encoding="utf-8")
    return found


def title_of(src: Path) -> str:
    for line in src.read_text(encoding="utf-8").splitlines():
        if line.startswith("# # "):
            return line[4:].strip()
    return src.stem


def main() -> int:
    page = [
        "# Diagramas",
        "",
        "> Gerado por `scripts/build_diagramas.py` a partir dos notebooks. O visualizador de notebooks do"
        " GitHub não renderiza Mermaid; esta página, sim. Não editar à mão.",
        "",
    ]
    total = 0
    for src in sorted(SRC.glob("[0-9][0-9]_*.py")):
        diagrams = process(src)
        if not diagrams:
            continue
        link = f"Notebook: [`{src.stem}.ipynb`](../notebooks/{src.stem}.ipynb)"
        page += [f"## {title_of(src)}", "", link, ""]
        for anchor, heading, body in diagrams:
            total += 1
            page += [f'<a id="{anchor}"></a>', "", f"### {heading}", "", "```mermaid", body, "```", ""]
    OUT.write_text("\n".join(page), encoding="utf-8")
    print(f"ok  {OUT.relative_to(ROOT)}: {total} diagramas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
