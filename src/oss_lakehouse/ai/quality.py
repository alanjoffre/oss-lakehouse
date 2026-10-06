"""Caso de uso 3 — regras de qualidade sugeridas por LLM, aprovadas por humano, executadas no Spark.

Fluxo (o LLM nunca escreve código que roda):

    profiling ──► LLM propõe JSON ──► validação pydantic ──► compilador (allowlist de 6 tipos)
                                                                   │
                     👤 aprovação explícita (default = rejeitar) ◄─┘
                                   │
                     regras aprovadas ──► Column do Spark ──► linhas reprovadas por regra

O compilador é o guardrail: não existe tipo "expressão SQL livre", então uma regra maliciosa
ou alucinada não vira `DROP TABLE` — vira erro de compilação, visível na revisão.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from oss_lakehouse.ai.client import DEFAULT_MODEL, LLMClient, LLMRequest, LLMResponse, completar
from oss_lakehouse.ai.prompts import carregar_prompt
from oss_lakehouse.governance import mascara_formato_py

NUMERICOS = ("int", "bigint", "smallint", "tinyint", "double", "float", "decimal")


class Regra(BaseModel):
    """Regra de qualidade proposta pelo LLM: um dos 6 tipos de `regra`; o resto são parâmetros do tipo."""

    nome: str = Field(pattern=r"^[a-z0-9_]+$")
    coluna: str
    regra: Literal["not_null", "unique", "accepted_values", "regex", "range", "max_length"]
    valores: list[str] = []
    padrao: str = ""
    minimo: float | None = None
    maximo: float | None = None
    severidade: Literal["fail", "drop", "warn"]
    justificativa: str = ""


class SugestaoRegras(BaseModel):
    """Saída validada do prompt `gerar_regras_qualidade`: a lista de regras propostas."""

    regras: list[Regra]


class RegraInvalida(ValueError):
    """A regra passou no pydantic mas não compila (coluna inexistente, regex inválida, range sem limite…)."""


# --- profiling -----------------------------------------------------------------


def profiling(df: DataFrame, colunas_pessoais: Sequence[str] = (), max_top: int = 20) -> list[dict[str, Any]]:
    """Estatísticas por coluna numa passada (+1 groupBy por coluna de baixa cardinalidade).

    Coluna pessoal: mínimo/máximo/amostras saem com máscara de formato e sem valores frequentes.
    """
    tipos = dict(df.dtypes)
    aggs = [F.count(F.lit(1)).alias("__n")]
    for c in df.columns:
        aggs += [
            F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulos"),
            F.countDistinct(F.col(c)).alias(f"{c}__distintos"),
            F.min(F.col(c)).cast("string").alias(f"{c}__min"),
            F.max(F.col(c)).cast("string").alias(f"{c}__max"),
            F.min(F.length(F.col(c).cast("string"))).alias(f"{c}__min_len"),
            F.max(F.length(F.col(c).cast("string"))).alias(f"{c}__max_len"),
        ]
    linha = df.agg(*aggs).first()
    assert linha is not None
    n = linha["__n"]
    perfil = []
    for c in df.columns:
        pessoal = c in colunas_pessoais
        mn, mx = linha[f"{c}__min"], linha[f"{c}__max"]
        item: dict[str, Any] = {
            "coluna": c,
            "tipo": tipos[c],
            "linhas": n,
            "nulos": linha[f"{c}__nulos"],
            "distintos": linha[f"{c}__distintos"],
            "min": mascara_formato_py(mn) if pessoal and mn is not None else mn,
            "max": mascara_formato_py(mx) if pessoal and mx is not None else mx,
            "min_len": linha[f"{c}__min_len"],
            "max_len": linha[f"{c}__max_len"],
            "dado_pessoal": pessoal,
        }
        if not pessoal and linha[f"{c}__distintos"] <= max_top:
            top = df.groupBy(c).count().orderBy(F.desc("count"), F.col(c).asc_nulls_last()).collect()
            item["valores_frequentes"] = {str(r[c]): r["count"] for r in top}
        perfil.append(item)
    return perfil


def request_regras(
    tabela: str, contexto: str, perfil: list[dict[str, Any]], model: str = DEFAULT_MODEL
) -> LLMRequest:
    """Monta o pedido do prompt `gerar_regras_qualidade` com o profiling em JSON. Não chama o LLM."""
    return carregar_prompt("gerar_regras_qualidade").request(
        model=model,
        max_tokens=6000,
        tabela=tabela,
        contexto=contexto,
        profiling=json.dumps(perfil, ensure_ascii=False, indent=0, default=str),
    )


def sugerir_regras(
    client: LLMClient, tabela: str, contexto: str, perfil: list[dict[str, Any]], model: str = DEFAULT_MODEL
) -> tuple[list[Regra], LLMResponse]:
    """Chama o LLM e devolve as regras propostas (validadas, ainda NÃO aprovadas) + a resposta."""
    saida, resp = completar(client, request_regras(tabela, contexto, perfil, model), SugestaoRegras)
    return saida.regras, resp


# --- compilação -----------------------------------------------------------------


def compilar(regra: Regra, tipos: Mapping[str, str]) -> Column | None:
    """Regra → condição booleana (True = linha passa). `unique` não é por linha: devolve None.

    Nulo passa em todas as regras exceto `not_null` — cada regra testa uma coisa só.
    """
    if regra.coluna not in tipos:
        raise RegraInvalida(f"{regra.nome}: coluna {regra.coluna!r} não existe")
    c = F.col(regra.coluna)
    tipo = tipos[regra.coluna]
    if regra.regra == "not_null":
        return c.isNotNull()
    if regra.regra == "unique":
        return None
    if regra.regra == "accepted_values":
        if not regra.valores:
            raise RegraInvalida(f"{regra.nome}: accepted_values sem valores")
        return c.isNull() | c.cast("string").isin(list(regra.valores))
    if regra.regra == "regex":
        try:
            re.compile(regra.padrao)
        except re.error as exc:
            raise RegraInvalida(f"{regra.nome}: regex inválida ({exc})") from exc
        if not regra.padrao:
            raise RegraInvalida(f"{regra.nome}: regex vazia")
        return c.isNull() | c.cast("string").rlike(regra.padrao)
    if regra.regra == "range":
        if not tipo.startswith(NUMERICOS):
            raise RegraInvalida(f"{regra.nome}: range exige coluna numérica ({regra.coluna} é {tipo})")
        if regra.minimo is None and regra.maximo is None:
            raise RegraInvalida(f"{regra.nome}: range sem limites")
        cond = F.lit(True)
        if regra.minimo is not None:
            cond = cond & (c >= regra.minimo)
        if regra.maximo is not None:
            cond = cond & (c <= regra.maximo)
        return c.isNull() | cond
    if regra.regra == "max_length":
        if regra.maximo is None:
            raise RegraInvalida(f"{regra.nome}: max_length sem máximo")
        return c.isNull() | (F.length(c.cast("string")) <= int(regra.maximo))
    raise RegraInvalida(f"{regra.nome}: tipo {regra.regra} não suportado")  # pragma: no cover


# --- aprovação humana -------------------------------------------------------------

Decisao = Literal["aprovar", "rejeitar"] | dict[str, Any]


@dataclass(frozen=True)
class Revisao:
    """Resultado de `revisar`: as regras aprovadas e a trilha de auditoria de todas as decisões."""

    aprovadas: list[Regra]
    registro: list[dict[str, str]]  # trilha de auditoria: quem decidiu o quê


def revisar(
    regras: Sequence[Regra], decisoes: Mapping[str, Decisao], tipos: Mapping[str, str], revisor: str
) -> Revisao:
    """Aplica as decisões humanas. Sem decisão = rejeitada (default deny).

    Uma decisão pode ser um dict de ajustes (ex.: `{"severidade": "warn"}`) — aprova com edição.
    Regra que não compila é rejeitada mesmo se aprovada, e o motivo fica no registro.
    """
    aprovadas, registro = [], []
    for r in regras:
        d = decisoes.get(r.nome, "rejeitar")
        if d == "rejeitar":
            registro.append({"regra": r.nome, "decisao": "rejeitada", "por": revisor, "motivo": "revisor"})
            continue
        if isinstance(d, dict):
            r = r.model_copy(update=d)
        try:
            compilar(r, tipos)
        except RegraInvalida as exc:
            registro.append(
                {"regra": r.nome, "decisao": "rejeitada", "por": "compilador", "motivo": str(exc)}
            )
            continue
        aprovadas.append(r)
        registro.append(
            {
                "regra": r.nome,
                "decisao": "aprovada" + (" com ajuste" if isinstance(d, dict) else ""),
                "por": revisor,
                "motivo": "",
            }
        )
    return Revisao(aprovadas, registro)


def executar(df: DataFrame, regras: Sequence[Regra]) -> list[dict[str, Any]]:
    """Quantas linhas cada regra reprova. Regras por linha numa única passada; `unique` à parte."""
    tipos = dict(df.dtypes)
    por_linha = [(r, compilar(r, tipos)) for r in regras]
    aggs = [F.count(F.lit(1)).alias("__n")] + [
        F.sum(F.when(~cond, 1).otherwise(0)).alias(r.nome) for r, cond in por_linha if cond is not None
    ]
    linha = df.agg(*aggs).first()
    assert linha is not None
    n = linha["__n"]
    out = []
    for r, cond in por_linha:
        if cond is None:
            dup = df.where(F.col(r.coluna).isNotNull()).groupBy(r.coluna).count().where("count > 1")
            reprovadas = dup.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
        else:
            reprovadas = linha[r.nome]
        out.append(
            {
                "regra": r.nome,
                "coluna": r.coluna,
                "tipo": r.regra,
                "severidade": r.severidade,
                "reprovadas": int(reprovadas),
                "pct": round(100 * reprovadas / n, 3) if n else 0.0,
            }
        )
    return out
