"""Caso de uso 1 — classificar colunas com dado pessoal (PII) e sugerir tag/tratamento.

Guardrail central: o LLM **nunca vê valor real**. Ele recebe nome, tipo e amostras com máscara
de formato (`mascara_formato_py`: `ana@x.com` → `xxx@x.xxx`). O baseline de regex, que roda
dentro do perímetro, pode olhar o valor bruto — é a comparação justa do mundo real.

A tabela sintética de clientes usa só dado fictício: CPF gerado com dígito verificador válido,
e-mail em `example.com` (domínio reservado, RFC 2606) e IP da faixa de documentação 192.0.2.0/24
(RFC 5737) — nada aponta para uma pessoa ou servidor real.
"""

from __future__ import annotations

import json
import random
import re
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from typing import Literal

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from oss_lakehouse.ai.client import DEFAULT_MODEL, LLMClient, LLMRequest, LLMResponse, completar
from oss_lakehouse.ai.prompts import carregar_prompt
from oss_lakehouse.governance import mascara_formato_py


class ColunaPerfil(BaseModel):
    """O que o LLM vê de uma coluna: nome, tipo e amostras já com máscara de formato — nunca valor real."""

    nome: str
    tipo: str
    amostras: list[str]  # já mascaradas


class ClassificacaoColuna(BaseModel):
    """Veredito do LLM para uma coluna: é PII?, categoria, classificação e tratamento sugerido."""

    nome: str
    pii: bool
    categoria: Literal[
        "identificador_direto",
        "identificador_indireto",
        "contato",
        "sensivel_lgpd",
        "texto_livre",
        "nao_pessoal",
    ]
    classificacao: Literal["publico", "interno", "confidencial", "restrito"]
    tratamento: Literal["nenhum", "hmac", "mascara", "tokenizacao", "criptografia", "remover"]
    justificativa: str


class RespostaPII(BaseModel):
    """Saída validada do prompt `classificar_pii`: uma classificação por coluna."""

    colunas: list[ClassificacaoColuna]


# --- perfil de colunas --------------------------------------------------------


def achatar(df: DataFrame) -> DataFrame:
    """Structs de 1 nível viram colunas `pai_filho` (actor.login → actor_login)."""
    cols = []
    for campo in df.schema.fields:
        if isinstance(campo.dataType, StructType):
            cols += [
                F.col(f"{campo.name}.{f.name}").alias(f"{campo.name}_{f.name}") for f in campo.dataType.fields
            ]
        else:
            cols.append(F.col(campo.name))
    return df.select(*cols)


def perfil_colunas(
    df: DataFrame,
    chave: str,
    n_amostras: int = 3,
    modulo: int = 50,
    max_chars: int = 60,
    sem_amostra: Sequence[str] = (),
) -> tuple[list[ColunaPerfil], dict[str, list[str]]]:
    """Perfil para o LLM (amostras mascaradas) e amostras brutas (só para o baseline local).

    Determinístico: subconjunto por `xxhash64(chave) % modulo == 0` e, por coluna, os valores
    distintos de menor sha256 — mesma tabela, mesmas amostras, mesma chave de cache.
    `sem_amostra`: colunas cujo valor muda a cada carga (ex.: `_ingested_at`) — só nome e tipo.
    """
    colunas = list(df.columns)
    sub = df.where(F.pmod(F.xxhash64(F.col(chave)), F.lit(modulo)) == 0).cache()
    perfis, brutas = [], {}
    tipos = dict(df.dtypes)
    for c in colunas:
        if c in sem_amostra:
            brutas[c] = []
            perfis.append(ColunaPerfil(nome=c, tipo=tipos[c], amostras=[]))
            continue
        valores = [
            r[0]
            for r in sub.select(F.substring(F.col(c).cast("string"), 1, max_chars).alias("v"))
            .where(F.col("v").isNotNull())
            .distinct()
            .orderBy(F.sha2(F.col("v"), 256))
            .limit(n_amostras)
            .collect()
        ]
        brutas[c] = valores
        perfis.append(ColunaPerfil(nome=c, tipo=tipos[c], amostras=[mascara_formato_py(v) for v in valores]))
    sub.unpersist()
    return perfis, brutas


def request_pii(
    tabela: str, contexto: str, perfis: Sequence[ColunaPerfil], model: str = DEFAULT_MODEL
) -> LLMRequest:
    """Monta o pedido do prompt `classificar_pii` com os perfis (já mascarados) em JSON. Não chama o LLM."""
    colunas = json.dumps([p.model_dump() for p in perfis], ensure_ascii=False, indent=0)
    return carregar_prompt("classificar_pii").request(
        model=model, max_tokens=4096, tabela=tabela, contexto=contexto, colunas=colunas
    )


def classificar_pii(
    client: LLMClient, tabela: str, contexto: str, perfis: Sequence[ColunaPerfil], model: str = DEFAULT_MODEL
) -> tuple[dict[str, ClassificacaoColuna], LLMResponse]:
    """Chama o LLM e devolve {nome da coluna: classificação} + a resposta.

    Coluna que o modelo omitir fica fora do dict; nome repetido na saída: vale o último.
    """
    saida, resp = completar(client, request_pii(tabela, contexto, perfis, model), RespostaPII)
    return {c.nome: c for c in saida.colunas}, resp


# --- baseline de regex ----------------------------------------------------------

PADROES_VALOR: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"^[\w.+-]+@[\w-]+(\.[\w-]+)+$"),
    "cpf": re.compile(r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$"),
    "telefone": re.compile(r"^(\+?55\s?)?\(?\d{2}\)?\s?9?\d{4}-?\d{4}$"),
    "ipv4": re.compile(r"^(\d{1,3}\.){3}\d{1,3}$"),
    "cep": re.compile(r"^\d{5}-?\d{3}$"),
    "url_de_usuario": re.compile(r"https?://[^ ]*(/users/|avatars)"),
}
PADRAO_NOME = re.compile(
    r"(nome|name|login|user|email|e_mail|cpf|cnpj|rg|telefone|phone|celular|endereco|address|cep|"
    r"nascimento|birth|avatar|author|actor|ip_|saude|health|sangue)",
    re.IGNORECASE,
)


def baseline_regex(nome: str, amostras_brutas: Sequence[str], usar_nome: bool = True) -> bool:
    """PII se algum valor casa um padrão conhecido (ou, opcionalmente, se o nome sugere)."""
    por_valor = any(p.search(v) for v in amostras_brutas for p in PADROES_VALOR.values())
    return por_valor or (usar_nome and bool(PADRAO_NOME.search(nome)))


# --- tabela sintética -----------------------------------------------------------

_NOMES = [
    "Ana",
    "Bruno",
    "Carla",
    "Diego",
    "Elisa",
    "Fábio",
    "Gabriela",
    "Heitor",
    "Isis",
    "João",
    "Lívia",
    "Marcos",
]
_SOBRENOMES = [
    "Silva",
    "Souza",
    "Oliveira",
    "Santos",
    "Pereira",
    "Lima",
    "Costa",
    "Ribeiro",
    "Almeida",
    "Gomes",
]
_CIDADES = [
    ("São Paulo", "SP"),
    ("Campinas", "SP"),
    ("Rio de Janeiro", "RJ"),
    ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"),
    ("Recife", "PE"),
    ("Porto Alegre", "RS"),
    ("Salvador", "BA"),
]
_OBS = [
    "cliente pediu 2ª via do boleto",
    "ligar no {tel} após 18h",
    "trocou endereço de entrega",
    "reclamação sobre atraso",
    "enviar contrato para {email}",
    "",
]


def cpf_ficticio(rng: random.Random) -> str:
    """CPF aleatório no formato `000.000.000-00`, com os dois dígitos verificadores válidos."""
    base = [rng.randint(0, 9) for _ in range(9)]
    for peso_ini in (10, 11):
        soma = sum(d * p for d, p in zip(base, range(peso_ini, 1, -1), strict=False))
        dv = (soma * 10) % 11
        base.append(0 if dv == 10 else dv)
    s = "".join(map(str, base))
    return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"


def tabela_clientes_sintetica(spark: SparkSession, n: int = 200, seed: int = 42) -> DataFrame:
    """DataFrame de `n` clientes fictícios (15 colunas, com e sem PII). Mesmo `seed` → mesmas linhas."""
    rng = random.Random(seed)
    linhas = []
    for k in range(n):
        nome = f"{rng.choice(_NOMES)} {rng.choice(_SOBRENOMES)}"
        email = f"{nome.split()[0].lower().replace('á', 'a').replace('í', 'i')}.{k}@example.com"
        tel = f"({rng.randint(11, 99)}) 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"
        cidade, uf = rng.choice(_CIDADES)
        linhas.append(
            (
                f"{rng.getrandbits(128):032x}",
                nome,
                cpf_ficticio(rng),
                email,
                tel,
                date(1950, 1, 1) + timedelta(days=rng.randint(0, 20000)),
                f"{rng.randint(10000, 99999)}-{rng.randint(0, 999):03d}",
                cidade,
                uf,
                rng.choice(["basico", "plus", "premium"]),
                round(rng.uniform(19.9, 199.9), 2),
                rng.choice(["A+", "A-", "B+", "O+", "O-", "AB+"]),
                f"192.0.2.{rng.randint(1, 254)}",
                rng.choice(_OBS).format(tel=tel, email=email),
                datetime(2026, 1, 1) + timedelta(minutes=rng.randint(0, 400000)),
            )
        )
    schema = (
        "id_cliente string, nome string, cpf string, email string, telefone string, data_nascimento date, "
        "cep string, cidade string, uf string, plano string, valor_mensal double, tipo_sanguineo string, "
        "ip_ultimo_acesso string, observacao string, criado_em timestamp"
    )
    return spark.createDataFrame(linhas, schema)
