"""Revisão humana dos gabaritos de IA, às cegas, com medida de concordância.

Por que às cegas: quem revisa vendo o rótulo existente (ou a resposta do modelo) tende a concordar
com ele (viés de ancoragem). E corrigir só os itens em que o modelo errou infla a métrica. O certo
é rotular sem ver nada e depois medir a concordância entre os dois rotuladores.

Uso:
    uv run python scripts/revisao_gabarito.py gerar   # cria as planilhas em evals/revisao_humana/
    uv run python scripts/revisao_gabarito.py medir   # lê o que foi preenchido e mede a concordância
"""

from __future__ import annotations

import argparse
import csv
import random
import sys

from oss_lakehouse.ai import evaluation as ev

OUT = ev.EVALS_DIR / "revisao_humana"
TITULOS = OUT / "titulos_as_cegas.csv"
PII = OUT / "pii_as_cegas.csv"
CATEGORIAS = ("bug", "feature", "docs", "chore", "outro")
PULAR = "nao_sei"
SEED = 20261005


def gerar() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    for path in (TITULOS, PII):
        if path.exists():
            print(f"{path.name} já existe — não sobrescrevo o que pode estar preenchido", file=sys.stderr)
            return 1
    titulos = ev.carregar_jsonl(ev.EVALS_DIR / "titles_gold.jsonl")
    # Ordem aleatória: quem rotular só os N primeiros ainda tem uma amostra aleatória.
    random.Random(SEED).shuffle(titulos)
    with TITULOS.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["i", "tipo", "titulo", "rotulo_humano"])
        for r in titulos:
            w.writerow([r["i"], "PR" if r["is_pr"] else "issue", r["title"], ""])
    with PII.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tabela", "coluna", "tipo", "amostras_mascaradas", "pii_humano"])
        for r in ev.carregar_jsonl(ev.EVALS_DIR / "pii_colunas_gold.jsonl"):
            w.writerow([r["tabela"], r["nome"], r["tipo"], " | ".join(r["amostras"][:3]), ""])
    print(f"ok  {TITULOS.relative_to(ev.EVALS_DIR.parent)} e {PII.relative_to(ev.EVALS_DIR.parent)}")
    return 0


def _relatorio(nome: str, gold: list, humano: list, itens: list[str], total: int, pulados: int) -> None:
    n = len(gold)
    acertos = sum(g == h for g, h in zip(gold, humano, strict=True))
    lo, hi = ev.intervalo_wilson(acertos, n)
    print(f"\n== {nome}: {n} de {total} rotulados ({pulados} pulados com '{PULAR}')")
    print(f"concordância bruta: {acertos}/{n} = {acertos / n:.3f}  (IC 95%: {lo:.2f}–{hi:.2f})")
    kappa = ev.kappa_cohen(gold, humano)
    print(f"kappa de Cohen:     {kappa:.3f}  (>0,8 forte · 0,6–0,8 boa · <0,6 rever o critério)")
    divergentes = [(i, g, h) for i, g, h in zip(itens, gold, humano, strict=True) if g != h]
    if divergentes:
        print("divergências (gabarito → humano):")
        for item, g, h in divergentes:
            print(f"  {g} → {h}   {item[:100]}")


def medir() -> int:
    if not TITULOS.exists() or not PII.exists():
        print("planilhas não encontradas: rode `gerar` primeiro", file=sys.stderr)
        return 1
    feito = False

    gold_t = {r["i"]: r for r in ev.carregar_jsonl(ev.EVALS_DIR / "titles_gold.jsonl")}
    linhas = list(csv.DictReader(TITULOS.open(encoding="utf-8")))
    preenchidas = [r for r in linhas if r["rotulo_humano"].strip()]
    aceitos = (*CATEGORIAS, PULAR)
    invalidas = [r for r in preenchidas if r["rotulo_humano"].strip().lower() not in aceitos]
    if invalidas:
        ids = [r["i"] for r in invalidas]
        print(f"rótulo inválido em i={ids}: use {CATEGORIAS} ou '{PULAR}'", file=sys.stderr)
        return 1
    validas = [r for r in preenchidas if r["rotulo_humano"].strip().lower() != PULAR]
    if validas:
        feito = True
        _relatorio(
            "títulos",
            [gold_t[int(r["i"])]["label"] for r in validas],
            [r["rotulo_humano"].strip().lower() for r in validas],
            [f"[i={r['i']}] {r['titulo']}" for r in validas],
            len(linhas),
            len(preenchidas) - len(validas),
        )

    colunas = ev.carregar_jsonl(ev.EVALS_DIR / "pii_colunas_gold.jsonl")
    gold_p = {(r["tabela"], r["nome"]): r["pii"] for r in colunas}
    linhas = list(csv.DictReader(PII.open(encoding="utf-8")))
    mapa = {"sim": True, "s": True, "nao": False, "não": False, "n": False}
    preenchidas = [r for r in linhas if r["pii_humano"].strip()]
    invalidas = [r for r in preenchidas if r["pii_humano"].strip().lower() not in (*mapa, PULAR)]
    if invalidas:
        nomes = [r["coluna"] for r in invalidas]
        print(f"valor inválido em {nomes}: use sim, nao ou '{PULAR}'", file=sys.stderr)
        return 1
    validas = [r for r in preenchidas if r["pii_humano"].strip().lower() != PULAR]
    if validas:
        feito = True
        _relatorio(
            "PII por coluna",
            [gold_p[(r["tabela"], r["coluna"])] for r in validas],
            [mapa[r["pii_humano"].strip().lower()] for r in validas],
            [f"{r['tabela']}.{r['coluna']}" for r in validas],
            len(linhas),
            len(preenchidas) - len(validas),
        )

    if not feito:
        print("nada preenchido ainda: preencha a última coluna das planilhas e rode de novo", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("comando", choices=["gerar", "medir"])
    return {"gerar": gerar, "medir": medir}[ap.parse_args().comando]()


if __name__ == "__main__":
    sys.exit(main())
