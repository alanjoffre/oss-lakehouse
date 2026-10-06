"""Preço por token (USD por 1 milhão de tokens, API direta da Anthropic, out/2026).

Fonte: tabela de modelos da documentação da Anthropic (consultada em 2026-10). Preço muda —
por isso está num lugar só e o notebook mostra a data. Batch API = 50% de desconto.
Atenção: modelos de gerações diferentes usam tokenizadores diferentes (o do Opus 4.7+ gera
até ~1,35x mais tokens para o mesmo texto), então "mesmo prompt" ≠ "mesmos tokens" entre famílias.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Preco:
    """Preço de um modelo em USD por 1 milhão de tokens (entrada e saída têm preços diferentes)."""

    entrada: float  # USD / 1M tokens de entrada
    saida: float  # USD / 1M tokens de saída


PRECOS: dict[str, Preco] = {
    "claude-haiku-4-5": Preco(1.00, 5.00),
    "claude-sonnet-5-5": Preco(2.00, 10.00),
    "claude-opus-5-5": Preco(4.00, 20.00),
}

DESCONTO_BATCH = 0.5


def custo_usd(modelo: str, tokens_entrada: int, tokens_saida: int, batch: bool = False) -> float:
    """Custo em USD de uma chamada, pela tabela `PRECOS`; `batch=True` aplica o desconto da Batch API.

    Modelo fora da tabela levanta `KeyError`.
    """
    p = PRECOS[modelo]
    custo = (tokens_entrada * p.entrada + tokens_saida * p.saida) / 1_000_000
    return custo * (DESCONTO_BATCH if batch else 1.0)


def custo_por_milhao_de_itens(
    modelo: str, tokens_entrada_por_item: float, tokens_saida_por_item: float, batch: bool = False
) -> float:
    """Custo de classificar 1 milhão de itens, dado o consumo médio medido por item."""
    return custo_usd(modelo, int(tokens_entrada_por_item * 1e6), int(tokens_saida_por_item * 1e6), batch)
