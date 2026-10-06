"""Caso de uso 4 — documentação automática: COMMENT de tabela e colunas, com revisão humana.

O comentário gerado vira metadado da tabela Delta (`ALTER TABLE ... ALTER COLUMN ... COMMENT`),
aparece no `DESCRIBE` e, no Unity Catalog, no Catalog Explorer e na busca. Texto vindo de LLM
é tratado como entrada não confiável: escapado antes de entrar no SQL e limitado em tamanho.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import BaseModel
from pyspark.sql import SparkSession

from oss_lakehouse.ai.client import DEFAULT_MODEL, LLMClient, LLMRequest, LLMResponse, completar
from oss_lakehouse.ai.prompts import carregar_prompt

MAX_TABELA, MAX_COLUNA = 300, 160


class ComentarioColuna(BaseModel):
    """Comentário proposto para uma coluna (texto do LLM ou do revisor, ainda sem escape de SQL)."""

    nome: str
    comentario: str


class DocTabela(BaseModel):
    """Saída validada do prompt `documentar_tabela`: comentário da tabela + um por coluna."""

    comentario_tabela: str
    colunas: list[ComentarioColuna]


def request_doc(
    tabela: str, contexto: str, perfil: list[dict[str, Any]], model: str = DEFAULT_MODEL
) -> LLMRequest:
    """Monta o pedido do prompt `documentar_tabela`; `perfil` entra como JSON. Não chama o LLM."""
    return carregar_prompt("documentar_tabela").request(
        model=model,
        max_tokens=4096,
        tabela=tabela,
        contexto=contexto,
        colunas=json.dumps(perfil, ensure_ascii=False, indent=0, default=str),
    )


def gerar_doc(
    client: LLMClient, tabela: str, contexto: str, perfil: list[dict[str, Any]], model: str = DEFAULT_MODEL
) -> tuple[DocTabela, LLMResponse]:
    """Chama o LLM e devolve a documentação validada + a resposta. Fora do contrato: `LLMOutputError`."""
    return completar(client, request_doc(tabela, contexto, perfil, model), DocTabela)


def revisar_doc(doc: DocTabela, ajustes: Mapping[str, str | None], colunas_reais: Sequence[str]) -> DocTabela:
    """Revisão humana: `ajustes[nome] = texto` substitui, `None` rejeita (coluna fica sem comentário).
    `ajustes["__tabela__"]` ajusta o comentário da tabela. Coluna que não existe é descartada."""
    cols = []
    for c in doc.colunas:
        if c.nome not in colunas_reais:
            continue
        texto = ajustes.get(c.nome, c.comentario)
        if texto:
            cols.append(ComentarioColuna(nome=c.nome, comentario=texto))
    return DocTabela(comentario_tabela=ajustes.get("__tabela__") or doc.comentario_tabela, colunas=cols)


def _literal_sql(texto: str, limite: int) -> str:
    """String SQL segura: uma linha, tamanho limitado, barra e aspas escapadas."""
    t = " ".join(texto.split())[:limite]
    return "'" + t.replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_comentarios(caminho: str, doc: DocTabela) -> list[str]:
    """Um `ALTER TABLE` para o comentário da tabela e um por coluna de `doc`. Só monta, não executa.

    O texto sai escapado e truncado (`MAX_TABELA`/`MAX_COLUNA`). O nome da coluna vem do modelo, então a
    crase dentro dele é duplicada: sem isso, um nome com crase fecharia o identificador e injetaria SQL.
    """
    alvo = f"delta.`{caminho}`"
    comentario = _literal_sql(doc.comentario_tabela, MAX_TABELA)
    sqls = [f"ALTER TABLE {alvo} SET TBLPROPERTIES ('comment' = {comentario})"]
    for c in doc.colunas:
        nome = c.nome.replace("`", "``")
        sqls.append(
            f"ALTER TABLE {alvo} ALTER COLUMN `{nome}` COMMENT {_literal_sql(c.comentario, MAX_COLUNA)}"
        )
    return sqls


def aplicar(spark: SparkSession, caminho: str, doc: DocTabela) -> int:
    """Executa os `ALTER TABLE` de `sql_comentarios` na tabela Delta e devolve quantos comandos rodou."""
    sqls = sql_comentarios(caminho, doc)
    for s in sqls:
        spark.sql(s)
    return len(sqls)
