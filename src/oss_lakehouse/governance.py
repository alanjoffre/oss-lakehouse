"""Governança e LGPD em PySpark: classificação, pseudonimização, mascaramento e esquecimento.

O que roda aqui é a parte que **não depende do Unity Catalog** — a transformação do dado.
No Databricks as mesmas ideias viram política declarativa (column mask, row filter, tags
governadas/ABAC); este módulo também gera o SQL dessas políticas para mostrar no notebook 11.

Quatro técnicas, do mais fraco ao mais forte em reversibilidade controlada:

| Técnica | Reversível? | Por quem? | Uso típico |
|---|---|---|---|
| hash sem sal (`sha2`) | **sim**, por dicionário | quem tem a lista de candidatos | nunca p/ dado pessoal |
| HMAC-SHA256 com chave | não (sem a chave) | só quem tem a chave recomputa | chave de junção pseudônima |
| tokenização (cofre) | sim | só quem lê o cofre | suporte, reidentificação auditada |
| criptografia (AES-GCM) | sim | só quem tem a chave | guardar o valor e poder abrir |

Regra de ouro: pseudonimizado **continua sendo dado pessoal** na LGPD (art. 13 §4º e art. 12):
quem tem a chave reidentifica. Só dado anonimizado de forma irreversível sai do escopo.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Classificação
# ---------------------------------------------------------------------------


class Classificacao(StrEnum):
    """Níveis de classificação da informação (padrão comum em política de segurança corporativa)."""

    PUBLICO = "publico"  # pode sair da empresa sem dano (ex.: nome de repositório público)
    INTERNO = "interno"  # uso interno, dano baixo se vazar (ex.: métricas operacionais)
    CONFIDENCIAL = "confidencial"  # dado pessoal comum, segredo de negócio
    RESTRITO = "restrito"  # dado pessoal sensível (LGPD art. 5º II), documento, credencial


Tratamento = Literal["nenhum", "hmac", "mascara", "tokenizacao", "criptografia", "remover"]


@dataclass(frozen=True)
class PoliticaColuna:
    """Política de uma coluna: o que ela é e como é exposta para quem não tem acesso aberto."""

    coluna: str
    classificacao: Classificacao
    dado_pessoal: bool
    tratamento: Tratamento = "nenhum"
    motivo: str = ""


# Política da bronze `gh_events`. O login do GitHub é público, mas é dado pessoal: identifica
# uma pessoa natural (LGPD art. 5º I). "Público" não tira o dado do escopo da lei (art. 7º §3º-§4º):
# o tratamento continua tendo de respeitar finalidade, boa-fé e interesse público que justificou
# a publicação.
POLITICA_GH_EVENTS: tuple[PoliticaColuna, ...] = (
    PoliticaColuna("id", Classificacao.PUBLICO, False),
    PoliticaColuna("type", Classificacao.PUBLICO, False),
    PoliticaColuna("actor_id", Classificacao.CONFIDENCIAL, True, "hmac", "id numérico identifica a conta"),
    PoliticaColuna("actor_login", Classificacao.CONFIDENCIAL, True, "hmac", "identificador direto"),
    PoliticaColuna("actor_url", Classificacao.CONFIDENCIAL, True, "remover", "deriva do login"),
    PoliticaColuna("actor_avatar_url", Classificacao.CONFIDENCIAL, True, "remover", "deriva do id"),
    PoliticaColuna("repo_name", Classificacao.INTERNO, True, "nenhum", "dono pode ser pessoa física"),
    PoliticaColuna("org_login", Classificacao.PUBLICO, False, "nenhum", "organização não é titular"),
    PoliticaColuna(
        "payload", Classificacao.CONFIDENCIAL, True, "remover", "texto livre e objetos de usuário"
    ),
    PoliticaColuna("created_at", Classificacao.PUBLICO, False),
)


def colunas_pessoais(politica: Sequence[PoliticaColuna]) -> list[str]:
    return [p.coluna for p in politica if p.dado_pessoal]


def aplicar_classificacao_delta(
    spark: SparkSession, caminho: str, politica: Sequence[PoliticaColuna]
) -> list[str]:
    """Grava a classificação como metadado da própria tabela Delta e devolve as colunas marcadas.

    Sem Unity Catalog não há tag governada; o que o Delta open source oferece é o **comentário de
    coluna** (fica no schema, aparece no `DESCRIBE`) e as **propriedades de tabela** (chave/valor
    livre, consultável por `SHOW TBLPROPERTIES`). Os dois viajam com a tabela no `_delta_log`.
    Limite honesto: é documentação — nada aqui *impede* a leitura. Coluna da política que não
    existe na tabela é ignorada.

    Para ler de volta use `ler_classificacao_delta`, não `SHOW TBLPROPERTIES` (ver o motivo lá).
    """
    existentes = set(spark.read.format("delta").load(caminho).columns)
    aplicadas = [p for p in politica if p.coluna in existentes]
    if not aplicadas:
        return []
    ordem = list(Classificacao)
    props = {
        "governanca.classificacao_maxima": max((p.classificacao for p in aplicadas), key=ordem.index).value,
        "governanca.contem_dado_pessoal": str(any(p.dado_pessoal for p in aplicadas)).lower(),
    }
    for p in aplicadas:
        pessoal = "sim" if p.dado_pessoal else "nao"
        texto = f"[classificacao={p.classificacao.value}; dado_pessoal={pessoal}; tratamento={p.tratamento}]"
        if p.motivo:
            texto += f" {p.motivo}"
        comentario = texto.replace("'", "''")
        spark.sql(f"ALTER TABLE delta.`{caminho}` ALTER COLUMN {p.coluna} COMMENT '{comentario}'")
        props[f"governanca.classificacao.{p.coluna}"] = p.classificacao.value
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(f"ALTER TABLE delta.`{caminho}` SET TBLPROPERTIES ({pares})")
    return [p.coluna for p in aplicadas]


def ler_classificacao_delta(spark: SparkSession, caminho: str) -> dict[str, str]:
    """Lê de volta as propriedades `governanca.*` — o que um job de auditoria varreria.

    Lê por `DESCRIBE DETAIL`, não por `SHOW TBLPROPERTIES`: este último passa pela redação de
    segredos do Spark (`spark.redaction.regex`: secret, password, token, url…) e devolve
    `*********(redacted)` quando a CHAVE ou o VALOR casam — `governanca.classificacao.actor_url`
    casa por causa do "url". Justamente as colunas que mais interessam sumiriam.
    """
    linha = spark.sql(f"DESCRIBE DETAIL delta.`{caminho}`").first()
    props: Mapping[str, str] = linha["properties"] if linha else {}
    return {k: v for k, v in props.items() if k.startswith("governanca.")}


def medir_k_anonimato(df: DataFrame, quase_identificadores: Sequence[str]) -> dict[str, int]:
    """Mede o k-anonimato: o tamanho do MENOR grupo com a mesma combinação de quase-identificadores.

    Quase-identificador (*quasi-identifier*) é a coluna que sozinha não identifica, mas combinada
    sim (hora + repositório + tipo de evento). k = 1 significa que existe ao menos uma linha única —
    reidentificável por quem conhece esses atributos, mesmo sem nome nenhum na tabela.
    Devolve k, o nº de grupos, quantos grupos são únicos e o total de linhas.
    """
    grupos = df.groupBy(*quase_identificadores).agg(F.count(F.lit(1)).alias("_n"))
    r = grupos.agg(
        F.min("_n").alias("k"),
        F.count(F.lit(1)).alias("grupos"),
        F.sum(F.when(F.col("_n") == 1, 1).otherwise(0)).alias("grupos_unicos"),
        F.sum("_n").alias("linhas"),
    ).first()
    if r is None or r["k"] is None:
        return {"k": 0, "grupos": 0, "grupos_unicos": 0, "linhas": 0}
    return {c: int(r[c]) for c in ("k", "grupos", "grupos_unicos", "linhas")}


# ---------------------------------------------------------------------------
# Segredos
# ---------------------------------------------------------------------------

DEMO_KEY = b"chave-de-demonstracao-nao-use-em-producao"


def obter_chave(env_var: str = "OSSLH_PSEUDO_KEY", permitir_demo: bool = False) -> bytes:
    """Chave de pseudonimização. Em produção vem do Key Vault (via secret scope), nunca do código.

    Local: variável de ambiente. Sem ela, só devolve a chave de demonstração se o chamador
    pedir explicitamente — falhar alto é melhor que pseudonimizar com chave conhecida.
    """
    valor = os.environ.get(env_var)
    if valor:
        return valor.encode()
    if permitir_demo:
        log.warning("usando a chave de DEMONSTRAÇÃO (%s não definida)", env_var)
        return DEMO_KEY
    raise RuntimeError(f"defina {env_var} (no Databricks: dbutils.secrets.get(scope, chave))")


# ---------------------------------------------------------------------------
# Hash, HMAC, tokenização, criptografia
# ---------------------------------------------------------------------------


def sha256_sem_sal(col: str | Column) -> Column:
    """Hash puro. Determinístico e SEM segredo: quem tem a lista de candidatos reverte."""
    return F.sha2(F.col(col) if isinstance(col, str) else col, 256)


def hmac_sha256(col: str | Column, chave: bytes) -> Column:
    """HMAC-SHA256 (RFC 2104) só com funções nativas do Spark — sem UDF Python.

    HMAC(K, m) = H((K ⊕ opad) || H((K ⊕ ipad) || m)), com K completada com zeros até 64 bytes
    (o tamanho de bloco do SHA-256). Como K é constante, (K ⊕ ipad) e (K ⊕ opad) viram literais
    binários e o cálculo inteiro roda na JVM, vetorizado — uma UDF Python custaria serialização
    linha a linha entre JVM e Python.
    """
    if len(chave) > 64:
        chave = hashlib.sha256(chave).digest()
    k = chave.ljust(64, b"\0")
    ipad = bytearray(b ^ 0x36 for b in k)
    opad = bytearray(b ^ 0x5C for b in k)
    c = F.col(col) if isinstance(col, str) else col
    interno = F.unhex(F.sha2(F.concat(F.lit(ipad), c.cast("string").cast("binary")), 256))
    return F.when(c.isNull(), F.lit(None)).otherwise(F.sha2(F.concat(F.lit(opad), interno), 256))


def hmac_sha256_py(valor: str, chave: bytes) -> str:
    """Mesma função em Python puro — referência para teste e para pseudonimizar no driver."""
    import hmac

    if len(chave) > 64:
        chave = hashlib.sha256(chave).digest()
    return hmac.new(chave, valor.encode(), hashlib.sha256).hexdigest()


def ataque_dicionario(
    hashes: DataFrame, col_hash: str, candidatos: DataFrame, col_candidato: str
) -> DataFrame:
    """Reverte hash sem sal: calcula sha256 de cada candidato e faz join com os hashes "protegidos".

    É o ataque que derruba o "a gente faz hash do login": a lista de candidatos (todos os logins
    do GitHub) é pública. Devolve (hash, valor_recuperado).
    """
    tabela_arco_iris = candidatos.select(
        F.sha2(F.col(col_candidato), 256).alias("_h"), F.col(col_candidato).alias("valor_recuperado")
    ).distinct()
    return (
        hashes.select(F.col(col_hash).alias("_h"))
        .distinct()
        .join(tabela_arco_iris, "_h")
        .withColumnRenamed("_h", col_hash)
    )


def construir_cofre_tokens(df: DataFrame, col: str) -> DataFrame:
    """Cofre de tokenização: um token aleatório por valor distinto.

    `uuid()` é NÃO determinístico: se o DataFrame for recomputado, os tokens mudam. Por isso o
    cofre tem de ser **gravado** (e lido de volta) antes de ser usado em join — e com acesso
    restrito, porque o cofre é o que reverte o token.
    """
    return (
        df.select(F.col(col).alias("valor"))
        .where(F.col("valor").isNotNull())
        .distinct()
        .withColumn(
            "token", F.concat(F.lit("tok_"), F.substring(F.regexp_replace(F.expr("uuid()"), "-", ""), 1, 16))
        )
    )


def tokenizar(df: DataFrame, col: str, cofre: DataFrame) -> DataFrame:
    """Troca o valor pelo token do cofre (left join: valor sem token vira nulo, nunca vaza)."""
    return (
        df.join(cofre.select(F.col("valor").alias(col), F.col("token").alias("_token")), col, "left")
        .withColumn(col, F.col("_token"))
        .drop("_token")
    )


def chaves_por_titular(df: DataFrame, col_titular: str) -> DataFrame:
    """Uma chave AES-256 aleatória por titular (base do crypto-shredding). Gravar antes de usar."""
    return (
        df.select(F.col(col_titular).alias("titular"))
        .where(F.col("titular").isNotNull())
        .distinct()
        .withColumn(
            "chave_hex",
            F.concat(
                F.regexp_replace(F.expr("uuid()"), "-", ""), F.regexp_replace(F.expr("uuid()"), "-", "")
            ),
        )
    )


def criptografar(col: str | Column, chave_hex: str | Column) -> Column:
    """AES-256-GCM (modo autenticado, padrão do `aes_encrypt`), em base64 para caber em STRING."""
    c = F.col(col) if isinstance(col, str) else col
    k = F.unhex(F.col(chave_hex) if isinstance(chave_hex, str) else chave_hex)
    return F.base64(F.aes_encrypt(c.cast("string"), k))


def descriptografar(col: str | Column, chave_hex: str | Column) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    k = F.unhex(F.col(chave_hex) if isinstance(chave_hex, str) else chave_hex)
    # try_aes_decrypt: sem a chave certa devolve NULL em vez de derrubar o job.
    return F.try_aes_decrypt(F.unbase64(c), k).cast("string")


# ---------------------------------------------------------------------------
# Mascaramento
# ---------------------------------------------------------------------------


def mascara_formato(col: str | Column) -> Column:
    """Mantém só o formato: maiúscula→X, minúscula→x, dígito→9, pontuação fica. `Ab3@x.io`→`Xx9@x.xx`."""
    c = F.col(col) if isinstance(col, str) else col
    return F.mask(c.cast("string"), F.lit("X"), F.lit("x"), F.lit("9"), F.lit(None))


def mascara_formato_py(valor: object) -> str:
    """Espelho Python de `mascara_formato` — usado antes de mandar amostra a um LLM."""
    if valor is None:
        return "NULL"
    out = []
    for ch in str(valor):
        if ch.isupper():
            out.append("X")
        elif ch.islower():
            out.append("x")
        elif ch.isdigit():
            out.append("9")
        else:
            out.append(ch)
    return "".join(out)


def mascara_parcial(col: str | Column, manter_fim: int = 2) -> Column:
    """`octocat` → `*****at`: o suporte confere o final sem ver o valor."""
    c = (F.col(col) if isinstance(col, str) else col).cast("string")
    escondidos = F.greatest(F.length(c) - manter_fim, F.lit(0))
    return F.when(c.isNull(), None).otherwise(
        F.concat(F.repeat(F.lit("*"), escondidos), F.right(c, F.lit(manter_fim)))
    )


def mascara_email(col: str | Column) -> Column:
    """`ana.silva@example.com` → `a***@example.com`: 1ª letra + domínio.

    O domínio fica aberto de propósito (serve para suporte e para métrica por provedor), mas em
    domínio pequeno ele sozinho identifica a pessoa — aí a máscara certa é `mascara_formato`.
    Valor sem `@` não é devolvido aberto: vira `***`.
    """
    c = (F.col(col) if isinstance(col, str) else col).cast("string")
    return (
        F.when(c.isNull(), None)
        .when(c.rlike("^[^@]+@[^@]+$"), F.regexp_replace(c, "(?<=^.)[^@]*(?=@)", "***"))
        .otherwise(F.lit("***"))
    )


def mascara_cpf(col: str | Column) -> Column:
    """`123.456.789-09` → `***.456.789-**`: esconde os 3 primeiros dígitos e os 2 verificadores.

    É o formato usado na publicação de atos oficiais no Brasil. Aceita o CPF com ou sem pontuação;
    o que não tiver 11 dígitos vira `***` (nunca devolve aberto o que não reconheceu).
    """
    c = (F.col(col) if isinstance(col, str) else col).cast("string")
    digitos = F.regexp_replace(c, "\\D", "")
    return (
        F.when(c.isNull(), None)
        .when(
            F.length(digitos) == 11,
            F.regexp_replace(digitos, "^\\d{3}(\\d{3})(\\d{3})\\d{2}$", "***.$1.$2-**"),
        )
        .otherwise(F.lit("***"))
    )


_RE_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+")
_RE_CPF = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")
_RE_TEL = re.compile(r"(\+?55\s?)?\(?\b\d{2}\)?\s?9?\d{4}[-\s]?\d{4}\b")
_RE_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_RE_MENCAO = re.compile(r"(?<![\w@])@[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})\b")


def limpar_texto_livre(texto: str) -> str:
    """Remove PII óbvia de texto livre antes de ele sair do perímetro (ex.: ir para um LLM).

    Regex pega o formato conhecido (e-mail, CPF, telefone, IP, @menção). Não pega nome próprio
    no meio da frase — para isso existe NER (reconhecimento de entidades); aqui fica explícito
    que é uma rede de proteção, não uma garantia.
    """
    texto = _RE_EMAIL.sub("<EMAIL>", texto)
    texto = _RE_CPF.sub("<CPF>", texto)
    texto = _RE_IPV4.sub("<IP>", texto)
    texto = _RE_TEL.sub("<TELEFONE>", texto)
    return _RE_MENCAO.sub("<USUARIO>", texto)


# ---------------------------------------------------------------------------
# Visão dinâmica por papel (o "column mask" sem Unity Catalog)
# ---------------------------------------------------------------------------

PAPEIS_ABERTOS = ("dpo", "suporte_privilegiado")


def _expr_tratada(p: PoliticaColuna, chave: bytes) -> Column | None:
    if p.tratamento == "nenhum":
        return F.col(p.coluna)
    if p.tratamento == "hmac":
        return hmac_sha256(p.coluna, chave)
    if p.tratamento == "mascara":
        return mascara_parcial(p.coluna)
    if p.tratamento == "remover":
        return None
    raise ValueError(f"tratamento {p.tratamento!r} exige cofre/chave por titular — use as funções dedicadas")


def aplicar_politica(
    df: DataFrame, politica: Sequence[PoliticaColuna], papel: str, chave: bytes
) -> DataFrame:
    """Devolve o DataFrame como o `papel` deve vê-lo: aberto para papéis privilegiados, tratado
    para os demais. Coluna sem política passa sem mudança (allowlist seria mais segura — ver nb 11)."""
    if papel in PAPEIS_ABERTOS:
        return df
    por_coluna = {p.coluna: p for p in politica}
    cols: list[Column] = []
    for nome in df.columns:
        p = por_coluna.get(nome)
        if p is None:
            cols.append(F.col(nome))
            continue
        e = _expr_tratada(p, chave)
        if e is not None:
            cols.append(e.alias(nome))
    return df.select(*cols)


def sql_hmac_sha256(expr: str, chave: bytes) -> str:
    """Expressão SQL equivalente a `hmac_sha256` (mesmo resultado, só funções nativas).

    A chave entra como literal binário (`X'..'`) — ou seja, fica no texto de quem usar a expressão.
    Serve para demonstração local; em produção a chave não pode morar na definição de uma visão.
    """
    if len(chave) > 64:
        chave = hashlib.sha256(chave).digest()
    k = chave.ljust(64, b"\0")
    ipad = bytes(b ^ 0x36 for b in k).hex().upper()
    opad = bytes(b ^ 0x5C for b in k).hex().upper()
    interno = f"unhex(sha2(concat(X'{ipad}', CAST(CAST({expr} AS STRING) AS BINARY)), 256))"
    return f"sha2(concat(X'{opad}', {interno}), 256)"


def sql_visao_dinamica(
    visao: str,
    origem: str,
    politica: Sequence[PoliticaColuna],
    variavel: str = "papel",
    chave: bytes | None = None,
) -> str:
    """SQL de uma visão que decide em tempo de consulta, pela variável de sessão `papel`.

    É o análogo local de `CASE WHEN is_account_group_member('pii_readers') THEN ... END` do
    Databricks. A visão é reavaliada a cada SELECT, então trocar o papel muda o resultado.
    Sem `chave`, o tratamento "hmac" sai como `sha2` puro (SQL legível, mas reversível por
    dicionário — só para leitura do exemplo). Com `chave`, sai o HMAC de verdade, idêntico ao de
    `hmac_sha256`; o preço é a chave ficar no texto da visão, aceitável só em demonstração.
    """
    abertos = ", ".join(f"'{p}'" for p in PAPEIS_ABERTOS)
    linhas = []
    for p in politica:
        if p.tratamento == "nenhum":
            linhas.append(f"  {p.coluna}")
        elif p.tratamento == "remover":
            linhas.append(f"  CASE WHEN {variavel} IN ({abertos}) THEN {p.coluna} END AS {p.coluna}")
        elif p.tratamento == "hmac":
            pseudo = sql_hmac_sha256(p.coluna, chave) if chave else f"sha2(CAST({p.coluna} AS STRING), 256)"
            linhas.append(
                f"  CASE WHEN {variavel} IN ({abertos}) THEN CAST({p.coluna} AS STRING) "
                f"ELSE {pseudo} END AS {p.coluna}"
            )
        else:
            linhas.append(
                f"  CASE WHEN {variavel} IN ({abertos}) THEN {p.coluna} "
                f"ELSE mask({p.coluna}, 'X', 'x', '9', NULL) END AS {p.coluna}"
            )
    return f"CREATE OR REPLACE TEMP VIEW {visao} AS SELECT\n" + ",\n".join(linhas) + f"\nFROM {origem}"


# ---------------------------------------------------------------------------
# SQL do Unity Catalog (mostrado no notebook; não roda local)
# ---------------------------------------------------------------------------


def sql_tags_uc(tabela: str, politica: Sequence[PoliticaColuna]) -> list[str]:
    """`ALTER TABLE ... ALTER COLUMN ... SET TAGS` — classificação vira metadado governado."""
    out = []
    for p in politica:
        tags = [f"'classificacao' = '{p.classificacao.value}'"]
        if p.dado_pessoal:
            tags.append("'pii' = 'true'")
        out.append(f"ALTER TABLE {tabela} ALTER COLUMN {p.coluna} SET TAGS ({', '.join(tags)});")
    return out


# ---------------------------------------------------------------------------
# LGPD: direito à eliminação (art. 18 VI) numa tabela Delta
# ---------------------------------------------------------------------------


def contar_no_parquet_bruto(spark: SparkSession, caminho: str, coluna: str, valor: str) -> int:
    """Conta o valor lendo TODOS os .parquet da pasta, ignorando o _delta_log.

    É a prova física: a versão atual da tabela pode não ter o dado, mas o arquivo antigo
    continua no disco até o VACUUM — e o time travel o encontra.
    """
    leitor = spark.read.option("pathGlobFilter", "*.parquet").option("recursiveFileLookup", "true")
    return leitor.parquet(caminho).where(F.col(coluna) == valor).count()


def arquivos_log_com_valor(caminho: str, valor: str) -> int:
    """Quantos arquivos do _delta_log contêm o valor em texto (estatísticas min/max por arquivo!)."""
    log_dir = Path(caminho) / "_delta_log"
    return sum(1 for f in log_dir.glob("*.json") if valor in f.read_text(errors="ignore"))


def vacuum_imediato(spark: SparkSession, caminho: str) -> None:
    """VACUUM com retenção zero — SÓ para demonstração.

    A checagem de retenção (padrão 7 dias) existe porque apagar arquivos ainda referenciados
    por leitores longos ou por streams quebra essas consultas e mata o time travel.
    Em produção: retenção curta o bastante para cumprir o prazo legal, nunca zero.
    """
    conf = "spark.databricks.delta.retentionDurationCheck.enabled"
    anterior = spark.conf.get(conf, "true")
    spark.conf.set(conf, "false")
    try:
        spark.sql(f"VACUUM delta.`{caminho}` RETAIN 0 HOURS")
    finally:
        spark.conf.set(conf, anterior)


def esquecer_titular(spark: SparkSession, caminho: str, condicao: str) -> dict[str, int]:
    """DELETE + REORG PURGE: remove as linhas e reescreve os arquivos que tinham deletion vector.

    Com deletion vectors o DELETE só marca a linha num arquivo .bin — o parquet original continua
    **referenciado** pela versão atual, então nem o VACUUM o remove. `REORG ... APPLY (PURGE)`
    reescreve esses arquivos sem as linhas; aí sim o VACUUM apaga o original.
    """
    spark.sql(f"DELETE FROM delta.`{caminho}` WHERE {condicao}")
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{caminho}` LIMIT 1").first()
    metricas: Mapping[str, str] = hist["operationMetrics"] if hist else {}
    spark.sql(f"REORG TABLE delta.`{caminho}` APPLY (PURGE)")
    return {
        "linhas_apagadas": int(metricas.get("numDeletedRows", 0)),
        "deletion_vectors": int(metricas.get("numDeletionVectorsAdded", 0)),
    }
