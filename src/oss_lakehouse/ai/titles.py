"""Caso de uso 2 — classificar títulos de issue/PR (bug/feature/docs/chore/outro).

Dois classificadores com a MESMA interface, para comparar com o mesmo gabarito:
- `baseline_palavras_chave`: regras determinísticas (custo zero, explicável, só inglês);
- `classificar_titulos`: LLM em lotes de 25 títulos por chamada (amortiza o prompt fixo).

Antes de sair do perímetro, cada título passa por `limpar_texto_livre` (e-mail, CPF, telefone,
IP e @menção viram marcadores) — título é texto livre e pode carregar dado pessoal.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel

from oss_lakehouse.ai.client import DEFAULT_MODEL, LLMClient, LLMRequest, LLMResponse, completar
from oss_lakehouse.ai.prompts import carregar_prompt
from oss_lakehouse.governance import limpar_texto_livre

CATEGORIAS: tuple[str, ...] = ("bug", "feature", "docs", "chore", "outro")
Categoria = Literal["bug", "feature", "docs", "chore", "outro"]
TAMANHO_LOTE = 25


class ItemTitulo(BaseModel):
    """Categoria que o LLM deu ao título de índice `i` do lote."""

    i: int
    categoria: Categoria


class LoteTitulos(BaseModel):
    """Saída validada do prompt `classificar_titulos` para um lote."""

    itens: list[ItemTitulo]


# --- baseline determinístico -------------------------------------------------

_DEP = re.compile(r"\b(bump|bumps|deps|dependency|dependencies|renovate|dependabot)\b")
_PREFIXO = re.compile(
    r"^\W{0,3}\[?(fix|feat|docs|chore|ci|test|tests|refactor|build|style)(\([^)]*\))?(\]|!?:)"
)
_PREFIXO_MAPA = {"fix": "bug", "feat": "feature", "docs": "docs"}
_DOCS = re.compile(r"\b(doc|docs|documentation|readme|typo|spec|specification)\b")
_BUG = re.compile(
    r"\b(fix|fixes|fixed|bug|error|errors|crash|crashes|fail|fails|failure|broken|regression|regressions|"
    r"cve|vulnerability|exception|wrong|incorrect|leak|doesn't|does not|don't|not working|never)\b"
)
_CHORE = re.compile(
    r"\b(refactor|cleanup|ci|release|version|test|tests|lint|housekeeping|sync|merge|update)\b"
)
_FEATURE = re.compile(
    r"\b(add|adds|added|support|implement|new|allow|enable|feature|introduce|improve|create|show|make)\b"
)


def baseline_palavras_chave(titulo: str) -> str:
    """Regra simples, na ordem: dependência → prefixo convencional → docs → bug → chore → feature → outro."""
    t = titulo.lower().strip()
    if _DEP.search(t):
        return "chore"
    m = _PREFIXO.match(t)
    if m:
        return _PREFIXO_MAPA.get(m.group(1), "chore")
    for regex, rotulo in ((_DOCS, "docs"), (_BUG, "bug"), (_CHORE, "chore"), (_FEATURE, "feature")):
        if regex.search(t):
            return rotulo
    return "outro"


def regra_de_alta_confianca(titulo: str) -> str | None:
    """Só as duas regras em que a convenção do título já É o rótulo (dependência e prefixo de
    conventional commits). `None` = a regra não se aplica → manda para o LLM.

    É o 1º degrau de uma **cascata** (regra barata primeiro, LLM só no que sobrar): corta chamadas
    sem depender de palavra-chave solta, que é onde o baseline erra.
    """
    t = titulo.lower().strip()
    if _DEP.search(t):
        return "chore"
    m = _PREFIXO.match(t)
    return _PREFIXO_MAPA.get(m.group(1), "chore") if m else None


# --- LLM ---------------------------------------------------------------------


def request_lote(itens: Sequence[tuple[int, str]], model: str = DEFAULT_MODEL) -> LLMRequest:
    """Um pedido para um lote: [(índice estável, título)]. Mesmo lote → mesma chave de cache."""
    payload = [{"i": i, "titulo": limpar_texto_livre(t)} for i, t in itens]
    return carregar_prompt("classificar_titulos").request(
        model=model, max_tokens=2048, itens=json.dumps(payload, ensure_ascii=False, indent=0)
    )


def lotes(titulos: Sequence[str], tamanho: int = TAMANHO_LOTE) -> list[list[tuple[int, str]]]:
    """Fatia os títulos em lotes de `tamanho`, cada item com o seu índice global: [(índice, título)]."""
    indexados = list(enumerate(titulos))
    return [indexados[k : k + tamanho] for k in range(0, len(indexados), tamanho)]


@dataclass
class Resultado:
    """Previsões alinhadas aos títulos de entrada (`None` = sem resposta válida) + a resposta de cada lote."""

    previsoes: list[str | None]
    respostas: list[LLMResponse] = field(default_factory=list)

    @property
    def sem_resposta(self) -> int:
        return sum(p is None for p in self.previsoes)


def classificar_titulos(
    client: LLMClient, titulos: Sequence[str], tamanho_lote: int = TAMANHO_LOTE, model: str = DEFAULT_MODEL
) -> Resultado:
    """Classifica em lotes. Item que o modelo esquecer ou inventar vira `None` (contado como erro)."""
    previsoes: list[str | None] = [None] * len(titulos)
    respostas = []
    for lote in lotes(titulos, tamanho_lote):
        saida, resp = completar(client, request_lote(lote, model), LoteTitulos)
        respostas.append(resp)
        validos = {i for i, _ in lote}
        for item in saida.itens:
            if item.i in validos:
                previsoes[item.i] = item.categoria
    return Resultado(previsoes, respostas)


def lote_adversarial(
    benignos: Sequence[str], ataques: Sequence[tuple[int, str]]
) -> tuple[list[tuple[int, str]], list[int | None]]:
    """Mistura títulos de ataque (prompt injection) num lote de títulos normais.

    `ataques` = [(posição no lote, título)]. Devolve o lote indexado e, para cada posição, o índice
    do título benigno de origem (ou `None` se for ataque) — para comparar com a previsão que o
    mesmo título recebeu num lote limpo.
    """
    titulos: list[str] = list(benignos)
    origem: list[int | None] = list(range(len(benignos)))
    for pos, titulo in sorted(ataques):
        titulos.insert(pos, titulo)
        origem.insert(pos, None)
    return list(enumerate(titulos)), origem
