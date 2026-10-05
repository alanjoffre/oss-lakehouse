"""Avaliação honesta de classificadores (LLM ou não) contra gabarito rotulado.

Métricas sem dependência externa (nada de sklearn): matriz de confusão, precisão/recall/F1
por classe, F1 macro, e intervalo de confiança de Wilson — com 120 exemplos, "84% de acurácia"
na verdade é "algo entre ~76% e ~89%". Dizer o intervalo é o que separa avaliação de marketing.

`GATES` são os limites mínimos usados como gate de CI (tests/test_ai_eval_gate.py): se uma
mudança de prompt/modelo derrubar a métrica abaixo do limite, o build falha.
"""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oss_lakehouse.config import PROJECT_ROOT

EVALS_DIR = PROJECT_ROOT / "evals"

# Limites do gate de REGRESSÃO. Foram fixados depois da 1ª medição (títulos 0,875; PII precisão
# 1,00 e recall 0,89), com margem para ruído de regravação — protegem contra piora, não provam
# qualidade. Ficam acima do baseline determinístico (títulos: 0,69): se o LLM não bate a regra
# simples, ele não paga o próprio custo.
GATES: dict[str, float] = {
    "classificar_titulos.acuracia": 0.80,
    "classificar_pii.recall": 0.85,
    "classificar_pii.precisao": 0.90,
}


def carregar_jsonl(path: str | Path) -> list[dict[str, Any]]:
    return [
        json.loads(linha) for linha in Path(path).read_text(encoding="utf-8").splitlines() if linha.strip()
    ]


def acuracia(gold: Sequence[Any], pred: Sequence[Any]) -> float:
    if len(gold) != len(pred) or not gold:
        raise ValueError("gold e pred precisam ter o mesmo tamanho (> 0)")
    return sum(g == p for g, p in zip(gold, pred, strict=True)) / len(gold)


def intervalo_wilson(acertos: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """IC de 95% para uma proporção (Wilson) — se comporta bem com n pequeno e p perto de 0/1."""
    if n == 0:
        return (0.0, 1.0)
    p = acertos / n
    den = 1 + z**2 / n
    centro = (p + z**2 / (2 * n)) / den
    meia = z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / den
    return (max(0.0, centro - meia), min(1.0, centro + meia))


def matriz_confusao(
    gold: Sequence[str], pred: Sequence[str | None], rotulos: Sequence[str]
) -> list[list[int]]:
    """Linhas = gabarito, colunas = previsto. Previsão ausente (None) conta na coluna extra 'sem_resposta'."""
    idx = {r: i for i, r in enumerate(rotulos)}
    m = [[0] * (len(rotulos) + 1) for _ in rotulos]
    for g, p in zip(gold, pred, strict=True):
        m[idx[g]][idx.get(p, len(rotulos)) if p is not None else len(rotulos)] += 1
    return m


def formatar_matriz(m: list[list[int]], rotulos: Sequence[str]) -> str:
    cab = ["gabarito \\ previsto", *rotulos, "sem_resp"]
    w = max(len(c) for c in cab)
    linhas = ["".join(c.rjust(w + 1) for c in cab)]
    for r, linha in zip(rotulos, m, strict=True):
        linhas.append(r.rjust(w + 1) + "".join(str(v).rjust(w + 1) for v in linha))
    return "\n".join(linhas)


@dataclass(frozen=True)
class MetricasClasse:
    precisao: float
    recall: float
    f1: float
    suporte: int


def metricas_por_classe(
    gold: Sequence[str], pred: Sequence[str | None], rotulos: Sequence[str]
) -> dict[str, MetricasClasse]:
    out = {}
    for r in rotulos:
        tp = sum(g == r and p == r for g, p in zip(gold, pred, strict=True))
        fp = sum(g != r and p == r for g, p in zip(gold, pred, strict=True))
        fn = sum(g == r and p != r for g, p in zip(gold, pred, strict=True))
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        out[r] = MetricasClasse(prec, rec, f1, tp + fn)
    return out


def f1_macro(gold: Sequence[str], pred: Sequence[str | None], rotulos: Sequence[str]) -> float:
    m = metricas_por_classe(gold, pred, rotulos)
    return sum(v.f1 for v in m.values()) / len(m)


@dataclass(frozen=True)
class Binario:
    tp: int
    fp: int
    fn: int
    tn: int

    @property
    def precisao(self) -> float:
        return self.tp / (self.tp + self.fp) if self.tp + self.fp else 0.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if self.tp + self.fn else 0.0


def binario(gold: Sequence[bool], pred: Sequence[bool]) -> Binario:
    pares = list(zip(gold, pred, strict=True))
    return Binario(
        tp=sum(g and p for g, p in pares),
        fp=sum((not g) and p for g, p in pares),
        fn=sum(g and (not p) for g, p in pares),
        tn=sum((not g) and (not p) for g, p in pares),
    )


def checar_gate(nome: str, valor: float) -> tuple[bool, str]:
    limite = GATES[nome]
    ok = valor >= limite
    return ok, f"{nome} = {valor:.3f} (limite {limite:.2f}) → {'OK' if ok else 'REPROVADO'}"
