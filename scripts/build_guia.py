"""Gera o GUIA_DE_ESTUDO.md a partir da seção "Perguntas de entrevista" de cada notebook.

Fonte única: a pergunta e a resposta moram no notebook; o guia é derivado (nunca editar à mão).

Uso: uv run python scripts/build_guia.py
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "notebooks" / "_src"
OUT = ROOT / "GUIA_DE_ESTUDO.md"

QUESTION = re.compile(r"^\*\*(\d+)\.\s*(.+?)\*\*\s*$")


@dataclass(frozen=True, slots=True)
class QA:
    number: int
    question: str
    answer: str


def markdown_lines(path: Path) -> list[str]:
    """Linhas das células markdown do fonte jupytext, sem o prefixo de comentário."""
    lines: list[str] = []
    in_markdown = False
    for raw in path.read_text(encoding="utf-8").splitlines():
        if raw.startswith("# %%"):
            in_markdown = "[markdown]" in raw
            continue
        if in_markdown:
            lines.append(raw[2:] if raw.startswith("# ") else raw.lstrip("#"))
    return lines


def parse(path: Path) -> tuple[str, list[QA]]:
    lines = markdown_lines(path)
    title = next((ln[2:].strip() for ln in lines if ln.startswith("# ")), path.stem)
    try:
        start = next(i for i, ln in enumerate(lines) if ln.startswith("## Perguntas de entrevista"))
    except StopIteration:
        return title, []
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))

    qas: list[QA] = []
    number, question, body = 0, "", []

    def flush() -> None:
        if question:
            text = "\n".join(body)
            text = re.sub(r"</?details>|<summary>.*?</summary>", "", text).strip()
            qas.append(QA(number, question, text))

    for ln in lines[start + 1 : end]:
        m = QUESTION.match(ln.strip())
        if m:
            flush()
            number, question, body = int(m.group(1)), m.group(2).strip(), []
        elif question:
            body.append(ln)
    flush()
    return title, qas


def main() -> int:
    sources = sorted(SRC.glob("[0-9][0-9]_*.py"))
    parsed = [(src, *parse(src)) for src in sources]
    total = sum(len(qas) for _, _, qas in parsed)

    out = [
        "# Guia de estudo",
        "",
        "> Gerado por `scripts/build_guia.py` a partir da seção **Perguntas de entrevista** de cada notebook."
        " Não editar à mão: mude a pergunta no notebook e rode `make guia`.",
        "",
        f"**{total} perguntas** em {len(parsed)} notebooks. Clique na pergunta para abrir a resposta curta;"
        " o notebook indicado tem a demonstração rodando e o aprofundamento.",
        "",
        "## Índice por tema",
        "",
        "| Notebook | Perguntas |",
        "|---|---|",
    ]
    for src, title, qas in parsed:
        anchor = src.stem.replace("_", "-")
        out.append(f"| [{title}](#{anchor}) | {len(qas)} |")
    out.append("")

    for src, title, qas in parsed:
        anchor = src.stem.replace("_", "-")
        link = f"Notebook: [`{src.stem}.ipynb`](notebooks/{src.stem}.ipynb)"
        out += [f'<a id="{anchor}"></a>', "", f"## {title}", "", link, ""]
        for qa in qas:
            out += [
                "<details>",
                f"<summary><b>{qa.number}. {qa.question}</b></summary>",
                "",
                qa.answer,
                "",
                "</details>",
                "",
            ]
    OUT.write_text("\n".join(out), encoding="utf-8")
    print(f"ok  {OUT.name}: {total} perguntas de {len(parsed)} notebooks")
    missing = [src.stem for src, _, qas in parsed if not qas]
    if missing:
        print(f"sem perguntas: {missing}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
