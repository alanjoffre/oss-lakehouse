"""Caso de uso 5 — triagem de falha: erro do Spark → categoria, causa provável e ação sugerida.

Antes de ir ao LLM, o erro é **normalizado**: caminhos viram `<caminho>`, ids de expressão do
plano (`id#123`) viram `#N`, UUIDs viram `<uuid>` e o stack trace da JVM é cortado. Dois motivos:
1. privacidade/segurança — caminho de storage, nome de conta e usuário não saem do perímetro;
   valor de dado citado na mensagem (`The value 'fulano/repo' ... cannot be cast`) vira `<valor>`:
   mensagem de erro carrega DADO, e dado pode ser pessoal;
2. cache e agrupamento — o mesmo erro em execuções diferentes gera o MESMO texto, logo a mesma
   chave de cache (paga uma vez) e dá para contar "quantas vezes esse erro aconteceu".
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel

from oss_lakehouse.ai.client import DEFAULT_MODEL, LLMClient, LLMRequest, LLMResponse, completar
from oss_lakehouse.ai.prompts import carregar_prompt

Categoria = Literal[
    "esquema_incompativel",
    "coluna_inexistente",
    "conversao_de_tipo",
    "dado_invalido",
    "arquivo_ausente",
    "recurso_memoria",
    "permissao",
    "configuracao",
    "outro",
]


class Triagem(BaseModel):
    """Saída validada do prompt `triagem_falha`: categoria, causa provável, ações e se precisa de humano."""

    categoria: Categoria
    causa_provavel: str
    acoes: list[str]
    confianca: Literal["baixa", "media", "alta"]
    precisa_humano: bool


_UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.IGNORECASE)
_EXPR_ID = re.compile(r"#\d+L?")
# Erros de dado do Spark citam o valor que falhou: [CAST_INVALID_INPUT] The value '...' of the type ...
_VALOR = re.compile(r"(?i)\b(values?|input|string)(:?\s+)'[^'\n]*'")
_CAMINHO = re.compile(r"(file:|abfss://|dbfs:|s3a?://)?(/[\w.\-=@]+){2,}/?")


def normalizar_erro(
    mensagem: str, raizes: tuple[str, ...] = (), max_linhas: int = 12, max_chars: int = 1500
) -> str:
    """Erro pronto para sair do perímetro e para servir de chave de cache: mesmo erro → mesmo texto.

    `raizes` viram `<raiz>`; stack trace da JVM sai; valor citado, UUID, id de expressão e caminho
    viram marcadores; o resultado é cortado em `max_linhas` e `max_chars`.
    """
    texto = mensagem
    for raiz in sorted(raizes, key=len, reverse=True):
        if raiz:
            texto = texto.replace(raiz, "<raiz>")
    linhas = [ln for ln in texto.splitlines() if not ln.lstrip().startswith("at ") and "\tat " not in ln]
    texto = "\n".join(linhas[:max_linhas])
    texto = _VALOR.sub(r"\1\2'<valor>'", texto)
    texto = _UUID.sub("<uuid>", texto)
    texto = _EXPR_ID.sub("#N", texto)
    texto = _CAMINHO.sub("<caminho>", texto)
    return texto[:max_chars].strip()


def request_triagem(job: str, contexto: str, erro: str, model: str = DEFAULT_MODEL) -> LLMRequest:
    """Monta o pedido do prompt `triagem_falha`. Nada é mascarado aqui: passe `erro` por `normalizar_erro`."""
    return carregar_prompt("triagem_falha").request(
        model=model, max_tokens=1500, job=job, contexto=contexto, erro=erro
    )


def triar(
    client: LLMClient, job: str, contexto: str, erro: str, model: str = DEFAULT_MODEL
) -> tuple[Triagem, LLMResponse]:
    """Chama o LLM e devolve a triagem validada + a resposta. Fora do contrato: `LLMOutputError`."""
    return completar(client, request_triagem(job, contexto, erro, model), Triagem)
