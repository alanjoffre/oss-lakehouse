# %% [markdown]
# # 16 · Live coding: SQL e PySpark
#
# > Prova que os 15 exercícios clássicos de entrevista de dados saem certos **nas duas linguagens** — Spark SQL
# > e DataFrame API — com um `assert` contra o resultado esperado, e que as mesmas soluções rodam na bronze real
# > do GH Archive (~279 mil eventos). Fecha com 3 exercícios de Python puro.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Python avançado | Exercícios 16–18 (sem pandas; arquivo maior que a memória) |
# | Databricks e processamento de dados | Exercícios 1–15 em Spark SQL **e** PySpark, aplicados à bronze |
# | Arquitetura de pipelines | Dedup (1–2), SCD2/as-of (8), sessionização (7), coorte (14) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Formato de cada exercício:** Enunciado → dados de exemplo (pequenos, criados na célula) → Solução SQL →
# Solução PySpark → `assert` provando que as duas batem com o esperado → aplicação na bronze (quando faz sentido)
# → Pegadinhas / complexidade / como explicar em voz alta.
#
# ### Como conduzir um live coding (o que o entrevistador avalia)
#
# 1. **Esclarecer antes de digitar:** grão da tabela, chave, empates, NULL, fuso horário, "e se vier duplicado?".
# 2. **Montar um exemplo pequeno com o caso de borda** (empate, NULL, intervalo exato) e dizer o resultado esperado.
# 3. **Solução direta primeiro**, falando em voz alta; depois otimizar.
# 4. **Testar contra o exemplo** — o `assert` deste notebook é isso.
# 5. **Falar de custo:** quantos shuffles, o que acontece com 1 bilhão de linhas, onde há skew.

# %% [markdown]
# ## Setup

# %%
import time
from datetime import date, datetime

from oss_lakehouse.config import get_settings
from oss_lakehouse.spark import get_spark
from oss_lakehouse.utils.live_coding import activity_streaks, dedup_latest, sessionize
from pyspark.sql import Window
from pyspark.sql import functions as F

spark = get_spark("16")
s = get_settings()

bronze = (
    spark.read.format("delta").load(s.path("bronze", "gh_events"))
    .withColumn("ts", F.to_timestamp("created_at"))
    .withColumn("is_bot", F.col("actor.login").endswith("[bot]"))
)
bronze.createOrReplaceTempView("bronze")
faixa = spark.sql("SELECT date_format(MIN(ts), 'yyyy-MM-dd HH:mm') AS ini, date_format(MAX(ts), 'HH:mm:ss') AS fim "
                  "FROM bronze").first()
print(f"bronze: {bronze.count():,} eventos | de {faixa.ini} a {faixa.fim} UTC")
# Pegadinha de fuso: o Spark calcula em UTC (spark.sql.session.timeZone), mas collect() devolve datetime
# do Python SEM fuso, convertido para o fuso local da máquina.
print("o mesmo MIN(ts) via collect():", bronze.agg(F.min("ts")).first()[0], "← fuso local do Python, não UTC")


def _k(row):  # chave de ordenação que tolera NULL
    return tuple((v is None, v if v is not None else 0) for v in row)


def confere(sql_df, api_df, esperado):
    """As duas soluções têm de devolver exatamente o esperado (ignorando a ordem das linhas)."""
    a = sorted((tuple(r) for r in sql_df.collect()), key=_k)
    b = sorted((tuple(r) for r in api_df.collect()), key=_k)
    e = sorted((tuple(r) for r in esperado), key=_k)
    assert a == e, f"SQL difere do esperado:\n{a}\n{e}"
    assert b == e, f"API difere do esperado:\n{b}\n{e}"
    print(f"ok: SQL == DataFrame API == esperado ({len(e)} linhas)")


# %% [markdown]
# ## 1. Deduplicar mantendo o registro mais recente por chave 🧪
#
# **Enunciado.** Uma tabela de atualizações de repositórios (estilo CDC) tem várias versões por `repo_id`.
# Devolva só a versão mais recente de cada repositório. Duas versões podem ter o mesmo `updated_at`: vale a de
# maior `seq` (ordem de chegada).

# %%
updates = spark.createDataFrame(
    [
        (1, "spark", 100, "2026-10-01 10:00:00", 1),
        (1, "spark", 120, "2026-10-01 12:00:00", 2),
        (1, "apache-spark", 130, "2026-10-01 12:00:00", 3),  # empate em updated_at: seq desempata
        (2, "delta", 50, "2026-10-01 09:00:00", 4),
        (3, "polars", None, "2026-10-01 11:00:00", 5),
        (3, "polars", 70, "2026-10-01 08:00:00", 6),  # mais antiga, mas chegou depois
    ],
    "repo_id int, name string, stars int, updated_at string, seq int",
).withColumn("updated_at", F.to_timestamp("updated_at"))
updates.createOrReplaceTempView("updates")
esperado = [(1, "apache-spark", 130), (2, "delta", 50), (3, "polars", None)]

# %% [markdown]
# **Solução SQL** — `ROW_NUMBER` numa janela por chave, ordenada do mais novo para o mais velho; `QUALIFY` filtra
# o resultado da janela sem subquery (Databricks SQL e Spark 4 suportam).

# %%
sql = spark.sql("""
    SELECT repo_id, name, stars
    FROM updates
    QUALIFY ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY updated_at DESC, seq DESC) = 1
""")

# %% [markdown]
# **Solução PySpark** — a função `dedup_latest` do pacote (`src/oss_lakehouse/utils/live_coding.py`, testada):

# %%
api = dedup_latest(updates, "repo_id", [F.col("updated_at").desc(), F.col("seq").desc()]).select("repo_id", "name", "stars")
confere(sql, api, esperado)

# alternativa sem janela: max_by com struct (1 agregação, sem ordenar a partição inteira)
alt = updates.groupBy("repo_id").agg(F.max_by(F.struct("name", "stars"), F.struct("updated_at", "seq")).alias("r"))
assert sorted(alt.select("repo_id", "r.name", "r.stars").collect(), key=_k) == sorted(esperado, key=_k)
print("ok: max_by(struct) dá o mesmo resultado")

# %% [markdown]
# **Na bronze:** último evento de cada repositório na janela de 3 horas.

# %%
ultimo = dedup_latest(bronze.withColumn("repo_id", F.col("repo.id")), "repo_id", [F.col("ts").desc(), F.col("id").desc()])
n_repos = bronze.select("repo.id").distinct().count()
print(f"repositórios distintos: {n_repos:,} | linhas após dedup: {ultimo.count():,}")
ultimo.select("repo.name", "type", "created_at").orderBy(F.col("ts").desc(), "repo.name").show(3, truncate=False)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - `ROW_NUMBER` e não `RANK`: com empate em `updated_at`, `RANK` devolveria 2 linhas para a mesma chave.
# - **Sem desempate determinístico** (`seq`, `id`), o resultado muda entre execuções — o pior tipo de bug.
# - `DISTINCT` não resolve: as linhas diferem em `stars`.
# - Custo: 1 shuffle por `repo_id` + ordenação dentro de cada partição. `max_by(struct)` evita a ordenação
#   completa (agregação parcial antes do shuffle) — melhor com muitas versões por chave.
# - Em voz alta: "particiono pela chave, ordeno do mais novo para o mais velho com desempate, fico com o 1º".
# - Em produção isso é o passo antes de um `MERGE` (notebook 05): o `MERGE` falha se a origem tiver 2 linhas
#   para a mesma chave.

# %% [markdown]
# ## 2. Encontrar duplicatas 🧪
#
# **Enunciado.** Liste os `order_id` que aparecem mais de uma vez, quantas vezes, e se são **cópias exatas**
# (reprocessamento) ou **conflito** (mesma chave, valores diferentes — problema de fonte).

# %%
pedidos = spark.createDataFrame(
    [(1, "ana", 10), (1, "ana", 10), (2, "bia", 20), (2, "bia", 25), (3, "caio", 30), (4, None, 5), (4, "duda", 5)],
    "order_id int, customer string, amount int",
)
pedidos.createOrReplaceTempView("pedidos")
esperado = [(1, 2, "cópia exata"), (2, 2, "conflito"), (4, 2, "conflito")]

# %%
sql = spark.sql("""
    SELECT order_id, COUNT(*) AS n,
           CASE WHEN COUNT(DISTINCT struct(customer, amount)) = 1 THEN 'cópia exata' ELSE 'conflito' END AS tipo
    FROM pedidos
    GROUP BY order_id
    HAVING COUNT(*) > 1
""")
api = (
    pedidos.groupBy("order_id")
    .agg(F.count("*").alias("n"), F.count_distinct(F.struct("customer", "amount")).alias("versoes"))
    .where("n > 1")
    .select("order_id", "n", F.when(F.col("versoes") == 1, "cópia exata").otherwise("conflito").alias("tipo"))
)
confere(sql, api, esperado)

errado = spark.sql("SELECT order_id, COUNT(DISTINCT customer, amount) AS v FROM pedidos WHERE order_id = 4 GROUP BY 1")
print("pegadinha — COUNT(DISTINCT a, b) ignora linha com NULL; pedido 4 tem 2 versões, mas conta:", errado.first()["v"])

# %% [markdown]
# **Na bronze:** pela chave técnica (`id`) e por uma chave "natural" (mesmo ator, repo, tipo e segundo).

# %%
dup_id = bronze.groupBy("id").count().where("count > 1").count()
dup_nat = spark.sql("""
    SELECT COUNT(*) AS chaves, SUM(n) AS linhas FROM (
      SELECT actor.id, repo.id, type, created_at, COUNT(*) AS n
      FROM bronze GROUP BY actor.id, repo.id, type, created_at HAVING COUNT(*) > 1)
""").first()
print(f"ids duplicados: {dup_id} | chaves naturais repetidas: {dup_nat['chaves']:,} ({dup_nat['linhas']:,} linhas)")
spark.sql("""
    SELECT type, COUNT(*) AS chaves_repetidas FROM (
      SELECT actor.id, repo.id, type, created_at FROM bronze
      GROUP BY ALL HAVING COUNT(*) > 1)
    GROUP BY type ORDER BY 2 DESC LIMIT 4
""").show()

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - A pergunta certa antes de responder: **"duplicata por qual chave?"**. Na bronze, `id` não repete, mas a
#   chave natural (ator, repo, tipo, segundo) repete — e isso **não é erro**: um ator pode fazer push em duas
#   branches, ou criar várias branches, no mesmo segundo. Deduplicar por essa chave apagaria eventos legítimos.
# - `COUNT(DISTINCT a, b)` descarta linhas com NULL em qualquer coluna; `COUNT(DISTINCT struct(a, b))` não.
# - `GROUP BY ALL` (Databricks SQL / Spark 4) agrupa por todas as colunas não agregadas.
# - Custo: 1 shuffle pela chave. Em tabela enorme, comece contando (`COUNT(*) - COUNT(DISTINCT id)`), que é barato.

# %% [markdown]
# ## 3. Top-N por grupo: `ROW_NUMBER` × `RANK` × `DENSE_RANK` 🧪
#
# **Enunciado.** Para cada tipo de evento, os 2 repositórios com mais eventos. **Empates entram** (se dois
# repositórios empatam no 2º lugar, os dois aparecem).

# %%
contagens = spark.createDataFrame(
    [("Push", "a", 10), ("Push", "b", 8), ("Push", "c", 8), ("Push", "d", 5),
     ("Watch", "x", 3), ("Watch", "y", 3), ("Watch", "z", 1)],
    "type string, repo string, n int",
)
contagens.createOrReplaceTempView("contagens")
spark.sql("""
    SELECT *, ROW_NUMBER() OVER w AS row_number, RANK() OVER w AS rank, DENSE_RANK() OVER w AS dense_rank
    FROM contagens WINDOW w AS (PARTITION BY type ORDER BY n DESC, repo)
""").show()
esperado = [("Push", "a", 10), ("Push", "b", 8), ("Push", "c", 8), ("Watch", "x", 3), ("Watch", "y", 3)]

# %%
sql = spark.sql("""
    SELECT type, repo, n FROM contagens
    QUALIFY RANK() OVER (PARTITION BY type ORDER BY n DESC) <= 2
""")
w = Window.partitionBy("type").orderBy(F.col("n").desc())
api = contagens.withColumn("r", F.rank().over(w)).where("r <= 2").select("type", "repo", "n")
confere(sql, api, esperado)

# %% [markdown]
# **Na bronze:** top-3 repositórios por tipo, para 3 tipos.

# %%
ranks = spark.sql("""
    SELECT type, repo.name AS repo, COUNT(*) AS n,           -- a janela roda depois do GROUP BY
           ROW_NUMBER() OVER (PARTITION BY type ORDER BY COUNT(*) DESC, repo.name) AS rn,
           RANK()       OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS rk,
           DENSE_RANK() OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS drk
    FROM bronze WHERE type IN ('WatchEvent', 'ForkEvent', 'ReleaseEvent')
    GROUP BY type, repo.name
""")
print("Quantas linhas o 'top-3' devolve por tipo, com cada função:")
ranks.groupBy("type").agg(
    F.sum((F.col("rn") <= 3).cast("int")).alias("row_number"),
    F.sum((F.col("rk") <= 3).cast("int")).alias("rank"),
    F.sum((F.col("drk") <= 3).cast("int")).alias("dense_rank"),
).orderBy("type").show()
ranks.where("type = 'WatchEvent' AND rn <= 3").select("repo", "n", "rk").orderBy("rn").show(truncate=False)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - `ROW_NUMBER`: exatamente N por grupo (empate decidido pelo desempate — ou ao acaso, se não houver).
#   `RANK`: empates dividem a posição e **pulam** a seguinte (1, 2, 2, 4). `DENSE_RANK`: não pula (1, 2, 2, 3) —
#   "top-2 valores distintos" pode devolver muitas linhas.
# - Pergunte: "se empatar no 2º lugar, quero 2 ou 3 linhas?". A resposta escolhe a função.
# - A janela pode usar a agregação direto (`RANK() OVER (... ORDER BY COUNT(*))`): o `GROUP BY` roda antes.
#   Neste Spark local, `QUALIFY` **junto com** `GROUP BY` falhou (`MISSING_GROUP_BY`); a subquery funciona em
#   qualquer motor.
# - Na bronze, com muitos repositórios empatados em 1 evento, `RANK`/`DENSE_RANK` chegam a devolver 176 linhas
#   num "top-3" (tabela acima) — é exatamente a conversa sobre empate que o entrevistador quer.
# - Custo: 1 shuffle por grupo + ordenação por grupo. Grupo gigante (skew) concentra numa tarefa; para top-N
#   global, `ORDER BY ... LIMIT N` usa *TakeOrderedAndProject* (top-N parcial por partição, sem ordenar tudo).

# %% [markdown]
# ## 4. Total acumulado e média móvel: frame `ROWS` × `RANGE` 🧪
#
# **Enunciado.** Para uma série diária de eventos (o dia 09-04 **não tem linha**), calcule o total acumulado e
# duas médias móveis: das **3 últimas linhas** e dos **3 últimos dias de calendário**.

# %%
diario = spark.createDataFrame(
    [("2026-09-01", 10), ("2026-09-02", 20), ("2026-09-03", 30), ("2026-09-05", 40), ("2026-09-06", 50)],
    "d string, n int",
).withColumn("d", F.to_date("d"))
diario.createOrReplaceTempView("diario")
esperado = [
    (date(2026, 9, 1), 10, 10.0, 10.0),
    (date(2026, 9, 2), 30, 15.0, 15.0),
    (date(2026, 9, 3), 60, 20.0, 20.0),
    (date(2026, 9, 5), 100, 30.0, 35.0),  # ROWS pega 03, 05 e... 02! RANGE pega só 03..05
    (date(2026, 9, 6), 150, 40.0, 45.0),
]

# %%
sql = spark.sql("""
    SELECT d,
           SUM(n) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acumulado,
           AVG(n) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS media_3_linhas,
           AVG(n) OVER (ORDER BY d RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW) AS media_3_dias
    FROM diario
""")
w = Window.orderBy("d")
w_dias = Window.orderBy(F.unix_date("d")).rangeBetween(-2, 0)  # RANGE na API: chave numérica (dias desde 1970)
api = diario.select(
    "d",
    F.sum("n").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)).alias("acumulado"),
    F.avg("n").over(w.rowsBetween(-2, 0)).alias("media_3_linhas"),
    F.avg("n").over(w_dias).alias("media_3_dias"),
)
confere(sql, api, esperado)

# %% [markdown]
# A pegadinha do frame **padrão**: com `ORDER BY` e sem frame explícito, o padrão é
# `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — linhas **empatadas** na ordenação entram juntas.

# %%
empate = spark.createDataFrame([("2026-09-01", 10), ("2026-09-01", 5), ("2026-09-02", 1)], "d string, n int")
empate.createOrReplaceTempView("empate")
spark.sql("""
    SELECT d, n,
           SUM(n) OVER (ORDER BY d) AS padrao_range,
           SUM(n) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_explicito
    FROM empate
""").show()

# %% [markdown]
# **Na bronze:** eventos por hora e acumulado (aqui sem `PARTITION BY`, todas as linhas vão para 1 partição — ok
# para 3 linhas agregadas, nunca para a tabela crua).

# %%
spark.sql("""
    SELECT date_trunc('hour', ts) AS hora, COUNT(*) AS eventos,
           SUM(COUNT(*)) OVER (ORDER BY date_trunc('hour', ts) ROWS UNBOUNDED PRECEDING) AS acumulado
    FROM bronze GROUP BY 1 ORDER BY 1
""").show(truncate=False)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **ROWS** conta linhas físicas; **RANGE** usa o valor da chave de ordenação. Com dia faltando, "média dos 3
#   últimos dias" é `RANGE`, não `ROWS` (ou preencha o calendário antes com uma dimensão de datas).
# - Frame padrão com empate soma as linhas empatadas juntas — sempre escreva o frame explicitamente.
# - Janela sem `PARTITION BY` = 1 partição só (o Spark avisa: *No Partition Defined for Window operation*).
#   Em tabela grande, particione (por dia, por chave) ou agregue antes.
# - Na API, `rangeBetween` exige chave numérica: `unix_date(d)` para data, `cast(ts as long)` para timestamp.

# %% [markdown]
# ## 5. `LAG`/`LEAD`: tempo entre eventos 🧪
#
# **Enunciado.** Para cada evento, quantos segundos se passaram desde o evento **anterior** do mesmo ator e até
# o **próximo**.

# %%
ev = spark.createDataFrame(
    [("ana", "2026-10-01 10:00:00"), ("ana", "2026-10-01 10:05:00"), ("ana", "2026-10-01 10:20:00"),
     ("bia", "2026-10-01 09:00:00"), ("bia", "2026-10-01 11:00:00")],
    "actor string, ts string",
).withColumn("ts", F.to_timestamp("ts"))
ev.createOrReplaceTempView("ev")
esperado = [
    ("ana", "10:00", None, 300), ("ana", "10:05", 300, 900), ("ana", "10:20", 900, None),
    ("bia", "09:00", None, 7200), ("bia", "11:00", 7200, None),
]

# %%
sql = spark.sql("""
    SELECT actor, date_format(ts, 'HH:mm') AS hora,
           unix_timestamp(ts) - unix_timestamp(LAG(ts) OVER w) AS seg_desde_anterior,
           unix_timestamp(LEAD(ts) OVER w) - unix_timestamp(ts) AS seg_ate_proximo
    FROM ev WINDOW w AS (PARTITION BY actor ORDER BY ts)
""")
w = Window.partitionBy("actor").orderBy("ts")
api = ev.select(
    "actor",
    F.date_format("ts", "HH:mm").alias("hora"),
    (F.unix_timestamp("ts") - F.unix_timestamp(F.lag("ts").over(w))).alias("seg_desde_anterior"),
    (F.unix_timestamp(F.lead("ts").over(w)) - F.unix_timestamp("ts")).alias("seg_ate_proximo"),
)
confere(sql, api, esperado)

# %% [markdown]
# **Na bronze:** intervalo mediano entre eventos dos 5 atores mais ativos (bots aparecem no topo — é o skew natural).

# %%
spark.sql("""
    WITH gaps AS (
      SELECT actor.login AS login,
             unix_timestamp(ts) - unix_timestamp(LAG(ts) OVER (PARTITION BY actor.id ORDER BY ts, id)) AS gap
      FROM bronze)
    SELECT login, COUNT(*) AS eventos, percentile_approx(gap, 0.5) AS gap_mediano_s, MAX(gap) AS maior_gap_s
    FROM gaps GROUP BY login ORDER BY eventos DESC LIMIT 5
""").show(truncate=False)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - O 1º evento de cada partição tem `LAG` nulo — decida se é 0, NULL ou "início" e diga isso em voz alta.
# - `LAG(ts, 1, default)` aceita valor padrão; `LAG(ts, 2)` olha 2 linhas atrás.
# - Diferença de timestamp: `unix_timestamp(a) - unix_timestamp(b)` (segundos) ou `timestampdiff(SECOND, b, a)`.
# - Custo: 1 shuffle por ator + ordenação. Ator gigante (o `github-actions[bot]`) concentra numa tarefa: skew.
#   O AQE não divide partição de janela; se doer, filtre/trate o bot separadamente.

# %% [markdown]
# ## 6. Gaps and islands: dias consecutivos de atividade 🧪
#
# **Enunciado.** Para cada ator, as sequências de dias consecutivos com atividade (início, fim, duração).
# Um ator pode ter vários eventos no mesmo dia.

# %%
atividade = spark.createDataFrame(
    [("ana", "2026-09-01"), ("ana", "2026-09-02"), ("ana", "2026-09-02"), ("ana", "2026-09-03"),
     ("ana", "2026-09-05"), ("bia", "2026-09-10"), ("bia", "2026-09-11")],
    "actor string, d string",
).withColumn("d", F.to_date("d"))
atividade.createOrReplaceTempView("atividade")
esperado = [
    ("ana", date(2026, 9, 1), date(2026, 9, 3), 3),
    ("ana", date(2026, 9, 5), date(2026, 9, 5), 1),
    ("bia", date(2026, 9, 10), date(2026, 9, 11), 2),
]

# %% [markdown]
# O truque: em dias consecutivos, `dia - ROW_NUMBER()` dá a mesma data — essa constante identifica a "ilha".
#
# ```text
# dia         row_number   dia - rn
# 2026-09-01       1      2026-08-31  ┐
# 2026-09-02       2      2026-08-31  ├ ilha 1 (3 dias)
# 2026-09-03       3      2026-08-31  ┘
# 2026-09-05       4      2026-09-01  ─ ilha 2 (1 dia)
# ```

# %%
sql = spark.sql("""
    WITH dias AS (SELECT DISTINCT actor, d FROM atividade),
         ilhas AS (SELECT actor, d, date_sub(d, ROW_NUMBER() OVER (PARTITION BY actor ORDER BY d)) AS ilha FROM dias)
    SELECT actor, MIN(d) AS inicio, MAX(d) AS fim, COUNT(*) AS dias
    FROM ilhas GROUP BY actor, ilha
""")
api = activity_streaks(atividade, "actor", "d")  # do pacote, testada
confere(sql, api, esperado)

# %% [markdown]
# **Na bronze** só há 1 dia; a mesma técnica vale para **minutos** consecutivos: maior sequência de minutos
# seguidos com atividade, por ator.

# %%
spark.sql("""
    WITH m AS (SELECT DISTINCT actor.login AS login, CAST(unix_timestamp(date_trunc('minute', ts)) / 60 AS BIGINT) AS minuto
               FROM bronze),
         ilhas AS (SELECT login, minuto - ROW_NUMBER() OVER (PARTITION BY login ORDER BY minuto) AS ilha FROM m)
    SELECT login, MAX(n) AS maior_sequencia_min FROM (SELECT login, ilha, COUNT(*) AS n FROM ilhas GROUP BY 1, 2)
    GROUP BY login ORDER BY 2 DESC, 1 LIMIT 5
""").show(truncate=False)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **Deduplicar antes** (`DISTINCT actor, d`): 2 eventos no mesmo dia quebram a conta (o `row_number` anda e o
#   dia não). Use `DENSE_RANK` se preferir não deduplicar.
# - Funciona com qualquer sequência inteira (minuto, número de versão); com datas, `date_sub`.
# - Variante "gaps": onde faltam dias → `LEAD(d) - d > 1`.
# - Custo: 1 shuffle por ator + 1 agregação. Em voz alta: "numero as linhas e subtraio do dia; o que é
#   consecutivo fica com a mesma diferença".

# %% [markdown]
# ## 7. Sessionização: nova sessão após mais de 30 minutos parado 🧪
#
# **Enunciado.** Atribua um número de sessão a cada evento: uma sessão termina quando o ator fica **mais de
# 30 minutos** sem evento (30 min exatos ainda é a mesma sessão).

# %%
cliques = spark.createDataFrame(
    [("u1", "2026-10-01 12:00:00"), ("u1", "2026-10-01 12:20:00"), ("u1", "2026-10-01 12:50:00"),
     ("u1", "2026-10-01 13:21:00"), ("u2", "2026-10-01 12:00:00")],
    "usuario string, ts string",
).withColumn("ts", F.to_timestamp("ts"))
cliques.createOrReplaceTempView("cliques")
esperado = [("u1", "12:00", 1), ("u1", "12:20", 1), ("u1", "12:50", 1), ("u1", "13:21", 2), ("u2", "12:00", 1)]

# %% [markdown]
# Padrão em 3 passos: `LAG` → flag "começou sessão" (1/0) → **soma acumulada** da flag = número da sessão.

# %%
sql = spark.sql("""
    WITH f AS (
      SELECT usuario, ts,
             CASE WHEN LAG(ts) OVER (PARTITION BY usuario ORDER BY ts) IS NULL
                    OR unix_timestamp(ts) - unix_timestamp(LAG(ts) OVER (PARTITION BY usuario ORDER BY ts)) > 30 * 60
                  THEN 1 ELSE 0 END AS nova
      FROM cliques)
    SELECT usuario, date_format(ts, 'HH:mm') AS hora,
           SUM(nova) OVER (PARTITION BY usuario ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sessao
    FROM f
""")
api = sessionize(cliques, "usuario", "ts", gap_minutes=30).select(
    "usuario", F.date_format("ts", "HH:mm").alias("hora"), F.col("session_n").alias("sessao")
)
confere(sql, api, esperado)

# %% [markdown]
# **Na bronze:** sessões por ator em 3 horas — distribuição e duração média das sessões de humanos.

# %%
sess = sessionize(bronze.select(F.col("actor.login").alias("login"), "ts", "is_bot"), "login", "ts", 30)
por_sessao = sess.groupBy("login", "is_bot", "session_n").agg(
    F.count("*").alias("eventos"), (F.max("ts").cast("long") - F.min("ts").cast("long")).alias("dur_s")
)
por_sessao.groupBy("is_bot").agg(
    F.count("*").alias("sessoes"),
    F.countDistinct("login").alias("atores"),
    F.round(F.avg("eventos"), 1).alias("eventos_por_sessao"),
    F.round(F.avg("dur_s") / 60, 1).alias("duracao_media_min"),
).show()

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **Limite exato**: "> 30" ou ">= 30"? Pergunte e teste o caso de 30 min cravados (está no exemplo).
# - Sessões que **cruzam a meia-noite** ou o limite do lote: em batch diário, a sessão é cortada. Solução:
#   processar com sobreposição (ler a última hora do dia anterior) ou sessionizar em streaming com
#   `session_window` (Spark 3.2+, notebook 06).
# - Ordenação estável: com 2 eventos no mesmo `ts`, acrescente um desempate (`id`).
# - Custo: 1 shuffle por usuário, 2 janelas sobre a mesma partição (o Spark reaproveita a ordenação).

# %% [markdown]
# ## 8. SCD tipo 2 e join *point-in-time* (as-of) 🧪
#
# **Enunciado.** Há um log de mudanças do plano de cada repositório (`free` → `pro` → ...). (a) Monte a dimensão
# **SCD2** (*Slowly Changing Dimension* tipo 2: uma linha por versão, com `valid_from`/`valid_to`). (b) Para cada
# evento, traga o plano **vigente no momento do evento** — não o atual.

# %%
mudancas = spark.createDataFrame(
    [(1, "free", "2026-01-01"), (1, "pro", "2026-03-01"), (1, "enterprise", "2026-06-01"), (2, "free", "2026-02-01")],
    "repo_id int, plan string, changed_at string",
).withColumn("changed_at", F.to_date("changed_at"))
eventos = spark.createDataFrame(
    [(1, "2026-02-15"), (1, "2026-03-01"), (1, "2026-07-01"), (2, "2026-01-15"), (2, "2026-05-05")],
    "repo_id int, ts string",
).withColumn("ts", F.to_date("ts"))
mudancas.createOrReplaceTempView("mudancas")
eventos.createOrReplaceTempView("eventos")
esperado = [
    (1, date(2026, 2, 15), "free"),
    (1, date(2026, 3, 1), "pro"),  # no dia exato da mudança já vale a nova versão (intervalo semiaberto)
    (1, date(2026, 7, 1), "enterprise"),
    (2, date(2026, 1, 15), None),  # antes da 1ª versão: não existe plano
    (2, date(2026, 5, 5), "free"),
]

# %% [markdown]
# **Solução SQL** — monta a SCD2 com `LEAD` (o fim de uma versão é o início da próxima) e faz o join pelo
# intervalo semiaberto `[valid_from, valid_to)`:

# %%
spark.sql("""
    CREATE OR REPLACE TEMP VIEW dim_repo_scd2 AS
    SELECT repo_id, plan, changed_at AS valid_from,
           LEAD(changed_at) OVER (PARTITION BY repo_id ORDER BY changed_at) AS valid_to,
           LEAD(changed_at) OVER (PARTITION BY repo_id ORDER BY changed_at) IS NULL AS is_current
    FROM mudancas
""")
spark.table("dim_repo_scd2").orderBy("repo_id", "valid_from").show()
sql = spark.sql("""
    SELECT e.repo_id, e.ts, d.plan
    FROM eventos e
    LEFT JOIN dim_repo_scd2 d
      ON e.repo_id = d.repo_id AND e.ts >= d.valid_from AND e.ts < COALESCE(d.valid_to, DATE'9999-12-31')
""")

# %% [markdown]
# **Solução PySpark** — *as-of join* direto do log, sem montar a SCD2: junta com todas as versões anteriores ao
# evento e fica com a mais recente.

# %%
cand = eventos.alias("e").join(
    mudancas.alias("m"), (F.col("e.repo_id") == F.col("m.repo_id")) & (F.col("m.changed_at") <= F.col("e.ts")), "left"
)
w = Window.partitionBy("e.repo_id", "e.ts").orderBy(F.col("m.changed_at").desc_nulls_last())
api = cand.withColumn("rn", F.row_number().over(w)).where("rn = 1").select("e.repo_id", "e.ts", "m.plan")
confere(sql, api, esperado)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **Intervalo semiaberto** `[from, to)`: com `BETWEEN` (fechado dos dois lados), o evento do dia da mudança casa
#   com 2 versões e duplica a linha do fato.
# - `valid_to` nulo para a versão atual: `COALESCE` com uma data "infinita" (ou grave `9999-12-31` direto).
# - Evento anterior à 1ª versão: `LEFT JOIN` mantém o evento com plano nulo — pergunte se deve ir para um
#   membro "desconhecido" da dimensão (prática Kimball, notebook 07).
# - Custo: o join por desigualdade (range join) é caro — vira *nested loop* ou explode candidatos. O as-of com
#   janela também gera N candidatos por evento. No Databricks, o *range join optimization* (`RANGE_JOIN` hint)
#   resolve intervalos; com chave de igualdade + intervalo (como aqui), o equi-join em `repo_id` já limita.
# - Construir a SCD2 incremental (com `MERGE`, fechando a versão antiga) está no notebook 05.

# %% [markdown]
# ## 9. Pivot e unpivot 🧪
#
# **Enunciado.** (a) Transforme (hora, tipo, n) em uma linha por hora com uma coluna por tipo. (b) Volte ao
# formato longo.

# %%
longo = spark.createDataFrame(
    [(12, "PushEvent", 5), (12, "WatchEvent", 2), (13, "PushEvent", 7), (14, "WatchEvent", 1)],
    "hora int, type string, n int",
)
longo.createOrReplaceTempView("longo")
esperado_pivot = [(12, 5, 2), (13, 7, None), (14, None, 1)]

# %%
sql = spark.sql("""
    SELECT * FROM longo
    PIVOT (SUM(n) FOR type IN ('PushEvent' AS push, 'WatchEvent' AS watch))
""")
api = longo.groupBy("hora").pivot("type", ["PushEvent", "WatchEvent"]).agg(F.sum("n")) \
    .withColumnsRenamed({"PushEvent": "push", "WatchEvent": "watch"})
confere(sql, api, esperado_pivot)
largo = api
largo.createOrReplaceTempView("largo")

# %%
esperado_unpivot = [(12, "push", 5), (12, "watch", 2), (13, "push", 7), (14, "watch", 1)]
sql = spark.sql("SELECT * FROM largo UNPIVOT (n FOR type IN (push, watch))")  # EXCLUDE NULLS é o padrão
api = largo.unpivot("hora", ["push", "watch"], "type", "n")
print("DataFrame.unpivot mantém os NULL:", api.where("n IS NULL").count(), "linha(s)")
confere(sql, api.where("n IS NOT NULL"), esperado_unpivot)

# %% [markdown]
# **Na bronze:** eventos por hora × 4 tipos principais.

# %%
spark.sql("""
    SELECT * FROM (SELECT hour(ts) AS hora, type FROM bronze)
    PIVOT (COUNT(*) FOR type IN ('PushEvent' AS push, 'CreateEvent' AS create_, 'PullRequestEvent' AS pr,
                                'WatchEvent' AS watch))
    ORDER BY hora
""").show()

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **Liste os valores do pivot** (`IN (...)` / `pivot(col, [valores])`): sem a lista, o Spark roda um job extra
#   para descobrir os valores distintos, e a quantidade de colunas passa a depender do dado (schema instável).
# - SQL `UNPIVOT` exclui NULL por padrão (`INCLUDE NULLS` para manter); `DataFrame.unpivot` mantém — os dois
#   lados divergem se você não filtrar.
# - Antes do Spark 3.4, unpivot se fazia com `stack(2, 'push', push, 'watch', watch)`.
# - Pivot com muitas colunas (milhares) estoura o plano; nesse caso mantenha o formato longo.

# %% [markdown]
# ## 10. Explode de array e JSON: `from_json`, `explode`, `posexplode` 🧪
#
# **Enunciado.** Cada issue tem um JSON com uma lista de labels. Gere uma linha por label, com a posição, e
# **mantenha as issues sem label** (com label nulo).

# %%
issues = spark.createDataFrame(
    [(1, '[{"name": "bug"}, {"name": "p1"}]'), (2, "[]"), (3, None)], "issue_id int, labels_json string"
)
issues.createOrReplaceTempView("issues")
esperado = [(1, 0, "bug"), (1, 1, "p1"), (2, None, None), (3, None, None)]
SCHEMA_LABELS = "array<struct<name: string>>"

# %%
sql = spark.sql(f"""
    SELECT issue_id, pos, lbl.name AS label
    FROM issues
    LATERAL VIEW OUTER posexplode(from_json(labels_json, '{SCHEMA_LABELS}')) t AS pos, lbl
""")
api = issues.select("issue_id", F.posexplode_outer(F.from_json("labels_json", SCHEMA_LABELS)).alias("pos", "lbl")) \
    .select("issue_id", "pos", F.col("lbl.name").alias("label"))
confere(sql, api, esperado)

so_explode = issues.select("issue_id", F.explode(F.from_json("labels_json", SCHEMA_LABELS))).count()
print(f"explode (sem _outer) devolve {so_explode} linhas: as issues 2 e 3 sumiram")

# %% [markdown]
# **Na bronze:** labels mais usadas nos `IssuesEvent` (o `payload` é STRING JSON na bronze).

# %%
labels = (
    bronze.where("type = 'IssuesEvent'")
    .select(F.explode(F.from_json(F.get_json_object("payload", "$.issue.labels"), SCHEMA_LABELS)).alias("l"))
    .groupBy(F.lower("l.name").alias("label")).count()
)
total_issues = bronze.where("type = 'IssuesEvent'").count()
print(f"IssuesEvent: {total_issues:,}")
labels.orderBy(F.desc("count"), "label").show(6, truncate=False)

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - `explode` **descarta** linhas com array vazio ou nulo; `explode_outer`/`posexplode_outer` mantêm.
# - `from_json` com JSON inválido devolve NULL silenciosamente (modo PERMISSIVE) — conte os nulos depois.
# - Schema explícito no `from_json` (`schema_of_json` ajuda a descobrir). No Databricks, com a coluna como
#   `VARIANT`, dá para navegar sem schema: `payload:issue.labels`.
# - Explode multiplica linhas: um array grande por linha vira *data explosion* — filtre antes, agregue logo depois.

# %% [markdown]
# ## 11. Semi-join × anti-join × `NOT IN` com NULL (a pegadinha) 🧪
#
# **Enunciado.** Dada uma lista de bloqueio, devolva (a) os atores que **estão** nela e (b) os que **não estão**.
# A lista tem um valor NULL (dado sujo) e um valor repetido.

# %%
atores = spark.createDataFrame([("ana",), ("bia",), ("caio",)], "login string")
bloqueio = spark.createDataFrame([("bia",), ("bia",), (None,)], "login string")
atores.createOrReplaceTempView("atores")
bloqueio.createOrReplaceTempView("bloqueio")

# %%
not_in = spark.sql("SELECT login FROM atores WHERE login NOT IN (SELECT login FROM bloqueio)")
print("NOT IN com NULL na subquery devolve:", not_in.count(), "linhas")

sql = spark.sql("SELECT login FROM atores a WHERE NOT EXISTS (SELECT 1 FROM bloqueio b WHERE b.login = a.login)")
api = atores.join(bloqueio, "login", "left_anti")
confere(sql, api, [("ana",), ("caio",)])

sql = spark.sql("SELECT login FROM atores a LEFT SEMI JOIN bloqueio b ON a.login = b.login")
api = atores.join(bloqueio, "login", "left_semi")
confere(sql, api, [("bia",)])
print("inner join no lugar do semi duplica a linha:", atores.join(bloqueio, "login").count(), "linhas para 'bia'")

# %% [markdown]
# **Na bronze:** repositórios que receberam estrela (`WatchEvent`) mas nenhum push na janela.

# %%
watch = bronze.where("type = 'WatchEvent'").select(F.col("repo.id").alias("rid")).distinct()
push = bronze.where("type = 'PushEvent'").select(F.col("repo.id").alias("rid"))
print(f"repos com estrela: {watch.count():,} | sem push: {watch.join(push, 'rid', 'left_anti').count():,} | "
      f"com push: {watch.join(push, 'rid', 'left_semi').count():,}")

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - `x NOT IN (lista com NULL)` vira `x <> NULL AND ...` → desconhecido → **nenhuma linha**. Isso é SQL padrão
#   (lógica de 3 valores), não bug do Spark. Use `NOT EXISTS` ou `LEFT ANTI JOIN`.
# - Semi-join devolve cada linha da esquerda **no máximo uma vez**; inner join duplica quando a direita repete.
# - O Spark converte `NOT IN` em *null-aware anti join*, que tende a ser mais caro; `NOT EXISTS` vira anti join
#   comum (hash/broadcast).
# - Lado pequeno (lista de bloqueio) → broadcast: sem shuffle do lado grande.

# %% [markdown]
# ## 12. Percentual do total 🧪
#
# **Enunciado.** Para cada tipo de evento, a quantidade e o percentual do total (1 casa decimal), e o percentual
# **dentro do grupo** bot/humano.

# %%
tipos = spark.createDataFrame(
    [("PushEvent", False, 4), ("PushEvent", True, 2), ("WatchEvent", False, 3), ("ForkEvent", False, 1)],
    "type string, is_bot boolean, n int",
)
tipos.createOrReplaceTempView("tipos")
esperado = [
    ("PushEvent", False, 4, 40.0, 50.0), ("PushEvent", True, 2, 20.0, 100.0),
    ("WatchEvent", False, 3, 30.0, 37.5), ("ForkEvent", False, 1, 10.0, 12.5),
]

# %%
sql = spark.sql("""
    SELECT type, is_bot, n,
           ROUND(100 * n / SUM(n) OVER (), 1) AS pct_total,
           ROUND(100 * n / SUM(n) OVER (PARTITION BY is_bot), 1) AS pct_no_grupo
    FROM tipos
""")
api = tipos.select(
    "type", "is_bot", "n",
    F.round(100 * F.col("n") / F.sum("n").over(Window.partitionBy()), 1).alias("pct_total"),
    F.round(100 * F.col("n") / F.sum("n").over(Window.partitionBy("is_bot")), 1).alias("pct_no_grupo"),
)
confere(sql, api, esperado)

# %% [markdown]
# **Na bronze** (agregando primeiro: a janela global opera sobre 16 linhas, não 279 mil):

# %%
spark.sql("""
    SELECT type, COUNT(*) AS n, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM bronze GROUP BY type ORDER BY n DESC LIMIT 5
""").show()
pct_bot = bronze.agg(F.round(100 * F.avg(F.col("is_bot").cast("int")), 1)).first()[0]
pct_gha = bronze.agg(F.round(100 * F.avg((F.col("actor.login") == "github-actions[bot]").cast("int")), 1)).first()[0]
print(f"eventos de bots: {pct_bot}% | só github-actions[bot]: {pct_gha}%")

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **Divisão inteira:** no Spark `/` devolve double; no SQL Server e no Postgres, `int / int` trunca (4/10 = 0).
#   Multiplique por `100.0` ou faça `CAST` — fale isso, mostra experiência com vários motores.
# - `SUM(...) OVER ()` sem partição joga tudo numa partição: agregue antes (como na bronze) ou faça `CROSS JOIN`
#   com o total calculado à parte.
# - Arredondar antes de somar faz o total dar 99,9% ou 100,1% — arredonde só na apresentação.
# - Média de booleano convertido para int = proporção (truque usado acima).

# %% [markdown]
# ## 13. Mediana e percentis: `percentile_approx` × exato 🧪
#
# **Enunciado.** Mediana e p90 de um conjunto **com quantidade par** de valores, de forma exata e aproximada.

# %%
valores = spark.createDataFrame([(v,) for v in [1, 2, 3, 4, 100, 200]], "v int")
valores.createOrReplaceTempView("valores")
esperado = [(3.5, 150.0, 3, 200)]

# %%
sql = spark.sql("""
    SELECT percentile(v, 0.5) AS mediana_exata, percentile(v, 0.9) AS p90_exato,
           percentile_approx(v, 0.5) AS mediana_aprox, percentile_approx(v, 0.9) AS p90_aprox
    FROM valores
""")
api = valores.agg(
    F.percentile("v", 0.5).alias("mediana_exata"),
    F.percentile("v", 0.9).alias("p90_exato"),
    F.percentile_approx("v", 0.5).alias("mediana_aprox"),
    F.percentile_approx("v", 0.9).alias("p90_aprox"),
)
confere(sql, api, esperado)
print("média:", valores.agg(F.avg("v")).first()[0], "— a média é puxada pelos extremos; a mediana não")

# %% [markdown]
# **Na bronze:** tamanho do `payload` (bytes) — exato × aproximado, com tempo medido.

# %%
tam = bronze.select(F.length("payload").alias("bytes"))
for nome, expr in [("exato", F.percentile("bytes", [0.5, 0.9, 0.99])),
                   ("aprox", F.percentile_approx("bytes", [0.5, 0.9, 0.99]))]:
    t0 = time.perf_counter()
    r = tam.agg(expr.alias("p")).first()["p"]
    print(f"{nome}: p50/p90/p99 = {[int(x) for x in r]}  ({time.perf_counter() - t0:.2f}s)")

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - `percentile` (exato) interpola: mediana de [1,2,3,4,100,200] = 3,5. `percentile_approx` devolve **um
#   elemento do conjunto** (3) — com par de valores, não bate com a mediana "de livro".
# - O exato guarda **todos os valores** de cada grupo na memória: com bilhões de linhas, OOM ou lentidão.
#   O aproximado usa um resumo de tamanho fixo (algoritmo de Greenwald-Khanna) controlado por `accuracy`
#   (padrão 10.000: erro relativo de rank ~1/10.000).
# - `median()` existe no Spark 3.4+ e no Databricks SQL (exato).
# - Em voz alta: "para dashboard e SLA uso o aproximado; para fechamento financeiro, exato — e conto o custo".

# %% [markdown]
# ## 14. Coorte de retenção 🧪
#
# **Enunciado.** Agrupe usuários pela **semana do primeiro acesso** (coorte) e calcule, para cada semana seguinte
# (0, 1, 2...), quantos voltaram e o percentual da coorte.

# %%
acessos = spark.createDataFrame(
    [("ana", "2026-09-01"), ("ana", "2026-09-09"), ("ana", "2026-09-16"),
     ("bia", "2026-09-02"),
     ("caio", "2026-09-03"), ("caio", "2026-09-17"),
     ("duda", "2026-09-08"), ("duda", "2026-09-15")],
    "usuario string, d string",
).withColumn("d", F.to_date("d"))
acessos.createOrReplaceTempView("acessos")
esperado = [
    (date(2026, 8, 31), 0, 3, 3, 100.0), (date(2026, 8, 31), 1, 1, 3, 33.3), (date(2026, 8, 31), 2, 2, 3, 66.7),
    (date(2026, 9, 7), 0, 1, 1, 100.0), (date(2026, 9, 7), 1, 1, 1, 100.0),
]

# %%
sql = spark.sql("""
    WITH a AS (SELECT DISTINCT usuario, CAST(date_trunc('week', d) AS DATE) AS semana FROM acessos),
         c AS (SELECT usuario, semana, MIN(semana) OVER (PARTITION BY usuario) AS coorte FROM a),
         r AS (SELECT coorte, CAST(datediff(semana, coorte) / 7 AS INT) AS semana_n, COUNT(*) AS ativos
               FROM c GROUP BY 1, 2)
    SELECT coorte, semana_n, ativos,
           FIRST_VALUE(ativos) OVER (PARTITION BY coorte ORDER BY semana_n) AS tamanho,
           ROUND(100 * ativos / FIRST_VALUE(ativos) OVER (PARTITION BY coorte ORDER BY semana_n), 1) AS pct
    FROM r
""")
a = acessos.select("usuario", F.date_trunc("week", "d").cast("date").alias("semana")).distinct()
primeira = a.groupBy("usuario").agg(F.min("semana").alias("coorte"))
tamanho = primeira.groupBy("coorte").agg(F.count("*").alias("tamanho"))
api = (
    a.join(primeira, "usuario")
    .groupBy("coorte", (F.datediff("semana", "coorte") / 7).cast("int").alias("semana_n"))
    .agg(F.count("*").alias("ativos"))
    .join(tamanho, "coorte")
    .select("coorte", "semana_n", "ativos", "tamanho", F.round(100 * F.col("ativos") / F.col("tamanho"), 1).alias("pct"))
)
confere(sql, api, esperado)
api.groupBy("coorte").pivot("semana_n").agg(F.first("pct")).orderBy("coorte").show()

# %% [markdown]
# **Na bronze:** coorte por **hora** do primeiro evento do ator (12h, 13h, 14h) e retenção nas horas seguintes.

# %%
spark.sql("""
    WITH a AS (SELECT DISTINCT actor.id AS ator, hour(ts) AS h FROM bronze),
         c AS (SELECT ator, h, MIN(h) OVER (PARTITION BY ator) AS coorte FROM a)
    SELECT * FROM (
      SELECT coorte, h - coorte AS hora_n,
             ROUND(100 * COUNT(*) / FIRST_VALUE(COUNT(*)) OVER (PARTITION BY coorte ORDER BY h - coorte), 1) AS pct
      FROM c GROUP BY coorte, h)
    PIVOT (FIRST(pct) FOR hora_n IN (0, 1, 2)) ORDER BY coorte
""").show()

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - **Defina "ativo"** (qualquer evento? só login?) e a **granularidade** (semana ISO começa na segunda —
#   `date_trunc('week')` do Spark também).
# - **Censura à direita:** a coorte mais nova ainda não teve tempo de chegar à semana 2 — célula vazia não é
#   retenção 0 (veja a coorte 09-07 na tabela acima, e a coorte das 14h na bronze).
# - Deduplicar usuário×semana antes de contar.
# - Custo: 1 shuffle por usuário para achar a coorte + 1 agregação pequena. Em produção, a coorte (data do 1º
#   acesso) é atributo da dimensão de usuário — calculada uma vez, não a cada consulta.

# %% [markdown]
# ## 15. `GROUP BY` com `ROLLUP`, `CUBE` e `GROUPING SETS` 🧪
#
# **Enunciado.** Contagem de eventos por tipo e por bot/humano, **com subtotais por tipo e total geral** numa só
# consulta. Um evento tem tipo NULL (dado sujo): o subtotal não pode ser confundido com ele.

# %%
eventos_g = spark.createDataFrame(
    [("PushEvent", True)] * 3 + [("PushEvent", False)] * 2 + [("WatchEvent", False), (None, False)],
    "type string, is_bot boolean",
)
eventos_g.createOrReplaceTempView("eventos_g")
esperado = [
    ("PushEvent", "true", 3), ("PushEvent", "false", 2), ("PushEvent", "TOTAL", 5),
    ("WatchEvent", "false", 1), ("WatchEvent", "TOTAL", 1),
    ("(nulo)", "false", 1), ("(nulo)", "TOTAL", 1),
    ("TOTAL", "TOTAL", 7),
]

# %%
sql = spark.sql("""
    SELECT CASE WHEN GROUPING(type) = 1 THEN 'TOTAL' ELSE COALESCE(type, '(nulo)') END AS tipo,
           CASE WHEN GROUPING(is_bot) = 1 THEN 'TOTAL' ELSE CAST(is_bot AS STRING) END AS bot,
           COUNT(*) AS n
    FROM eventos_g
    GROUP BY ROLLUP (type, is_bot)
""")
api = (
    eventos_g.rollup("type", "is_bot")
    .agg(F.count("*").alias("n"), F.grouping("type").alias("gt"), F.grouping("is_bot").alias("gb"))
    .select(
        F.when(F.col("gt") == 1, "TOTAL").otherwise(F.coalesce("type", F.lit("(nulo)"))).alias("tipo"),
        F.when(F.col("gb") == 1, "TOTAL").otherwise(F.col("is_bot").cast("string")).alias("bot"),
        "n",
    )
)
confere(sql, api, esperado)

n_rollup = spark.sql("SELECT 1 FROM eventos_g GROUP BY ROLLUP (type, is_bot)").count()
n_cube = spark.sql("SELECT 1 FROM eventos_g GROUP BY CUBE (type, is_bot)").count()
n_sets = spark.sql("SELECT 1 FROM eventos_g GROUP BY GROUPING SETS ((type), (is_bot), ())").count()
print(f"linhas — ROLLUP: {n_rollup} | CUBE: {n_cube} (acrescenta o subtotal por is_bot) | "
      f"GROUPING SETS ((type), (is_bot), ()): {n_sets}")

# %% [markdown]
# **Na bronze:** cubo tipo × bot, mostrando só os subtotais (`GROUPING_ID` identifica o nível).

# %%
spark.sql("""
    SELECT GROUPING_ID(type, is_bot) AS nivel, type, is_bot, COUNT(*) AS n
    FROM bronze GROUP BY CUBE (type, is_bot)
    HAVING GROUPING_ID(type, is_bot) > 0 AND (type IS NULL OR type IN ('PushEvent', 'CreateEvent'))
    ORDER BY nivel, n DESC
""").show()

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - O subtotal aparece como NULL — **igual** a um NULL de verdade no dado. `GROUPING(col)` (1 = linha de
#   subtotal) desfaz a ambiguidade; `GROUPING_ID` codifica o nível em bits.
# - `ROLLUP(a, b)` = conjuntos (a,b), (a), () — hierárquico. `CUBE(a, b)` = todas as combinações (2ⁿ).
#   `GROUPING SETS` = exatamente os conjuntos que você listar.
# - Custo: o Spark executa um `Expand` (replica cada linha uma vez por conjunto) antes de agregar — CUBE com
#   muitas colunas multiplica o volume do shuffle.
# - Útil para gerar a tabela agregada de um dashboard numa passada só.

# %% [markdown]
# ---
# # Python puro (sem pandas) 🧪
#
# ## 16. Merge de intervalos
#
# **Enunciado.** Dada uma lista de intervalos `[início, fim]` (fora de ordem), una os que se sobrepõem ou se tocam.
# Uso real: janelas de manutenção, períodos de assinatura, sessões que se cruzam.

# %%
from oss_lakehouse.utils.live_coding import merge_intervals

casos = {
    "clássico": ([(1, 3), (2, 6), (8, 10), (15, 18)], [(1, 6), (8, 10), (15, 18)]),
    "fora de ordem e encostando": ([(8, 10), (10, 12), (1, 3)], [(1, 3), (8, 12)]),
    "contido": ([(1, 10), (2, 3)], [(1, 10)]),
    "vazio": ([], []),
    "datas": ([(date(2026, 1, 5), date(2026, 1, 9)), (date(2026, 1, 1), date(2026, 1, 6))],
              [(date(2026, 1, 1), date(2026, 1, 9))]),
}
for nome, (entrada, esperado) in casos.items():
    assert merge_intervals(entrada) == esperado, nome
print("ok:", len(casos), "casos")

# %% [markdown]
# A solução (`utils/live_coding.py`), em 8 linhas:
#
# ```python
# def merge_intervals[T: SupportsLessThan](intervals):
#     merged = []
#     for start, end in sorted(intervals):                 # O(n log n)
#         if merged and not merged[-1][1] < start:         # começa antes do fim do último: sobrepõe/encosta
#             last_start, last_end = merged[-1]
#             merged[-1] = (last_start, end if last_end < end else last_end)   # max sem exigir __gt__
#         else:
#             merged.append((start, end))
#     return merged
# ```
#
# **Pegadinhas / complexidade / como explicar em voz alta**
# - Ordenar por início é o passo-chave; depois uma passada linear. Total O(n log n), memória O(n).
# - Pergunte: intervalos **fechados ou semiabertos**? `(1,4)` e `(4,5)` se unem se forem fechados; com `[1,4)` e
#   `[4,5)` também (encostam), mas `[1,4)` e `[5,6)` não.
# - O caso "contido" pega quem faz `merged[-1] = (início, end)` sem o `max`.
# - Genérico (PEP 695): funciona com int, data, timestamp — qualquer coisa comparável.

# %% [markdown]
# ## 17. Agrupar e contar sem pandas
#
# **Enunciado.** Do arquivo da hora 12, conte eventos por tipo e ache os 5 repositórios com mais eventos — só com
# a biblioteca padrão. Confira contra o Spark.

# %%
from collections import Counter, defaultdict

from oss_lakehouse.utils.io import iter_jsonl_gz
from oss_lakehouse.utils.live_coding import top_k

ARQ = f"{s.path('landing', 'gharchive')}/2026-10-01-12.json.gz"
t0 = time.perf_counter()
por_tipo: Counter[str] = Counter()
atores_por_repo: defaultdict[str, set[int]] = defaultdict(set)
repos: list[str] = []
for e in iter_jsonl_gz(ARQ):
    por_tipo[e["type"]] += 1
    repos.append(e["repo"]["name"])
    atores_por_repo[e["repo"]["name"]].add(e["actor"]["id"])
top5 = top_k(repos, 5)
t_py = time.perf_counter() - t0

spark_tipo = {r["type"]: r["count"] for r in bronze.where(F.col("_source_file").endswith("-12.json.gz"))
              .groupBy("type").count().collect()}
assert dict(por_tipo) == spark_tipo
print(f"{sum(por_tipo.values()):,} eventos em {t_py:.1f}s | por tipo bate com o Spark: ok")
for repo, n in top5:
    print(f"  {n:5,}  {repo}  ({len(atores_por_repo[repo])} atores distintos)")

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - `Counter` é um dict: memória O(chaves distintas), não O(linhas). `defaultdict(set)` evita o
#   `if chave not in d`.
# - Top-K: `Counter.most_common(k)` ou `heapq.nsmallest(k, ..., key=(-n, chave))` — O(n log k), sem ordenar tudo.
#   Defina o **desempate** (aqui, ordem alfabética), senão o resultado varia.
# - Acima a lista `repos` guarda todos os nomes para mostrar `top_k`; num arquivo enorme, conte direto num
#   `Counter` (exercício 18).

# %% [markdown]
# ## 18. Ler um arquivo enorme com memória limitada
#
# **Enunciado.** Encontre os 5 atores mais ativos em um conjunto de arquivos **cujas chaves distintas não cabem
# na memória**. (Aqui os 3 arquivos da landing cabem; simulamos a restrição e medimos o pico.)
#
# **Solução** — agregação externa por hash (`external_top_k`): 1ª passada espalha cada chave em N arquivos por
# um hash **estável** (a mesma chave sempre cai no mesmo arquivo); 2ª passada conta um arquivo por vez e mantém
# só os K melhores. Pico de memória ≈ maior partição + K. É o `groupBy` do Spark (shuffle por hash +
# agregação por partição) feito à mão.

# %%
import shutil
import tracemalloc
from pathlib import Path

from oss_lakehouse.utils.io import iter_jsonl_gz_many
from oss_lakehouse.utils.live_coding import external_top_k

ARQS = sorted(Path(s.path("landing", "gharchive")).glob("*.json.gz"))
DEMO16 = Path(s.data_root) / "demo" / "16"
SPILL = DEMO16 / "spill"
shutil.rmtree(SPILL, ignore_errors=True)
DEMO16.mkdir(parents=True, exist_ok=True)

# O "arquivo enorme": 1 login por linha, extraído dos 3 arquivos da landing (passada única, em streaming).
CHAVES = DEMO16 / "logins.txt"
with CHAVES.open("w") as f:
    for e in iter_jsonl_gz_many(ARQS):
        f.write(e["actor"]["login"] + "\n")
print(f"{CHAVES.name}: {CHAVES.stat().st_size / 1e6:.1f} MB")


def logins():
    with CHAVES.open() as f:
        for linha in f:
            yield linha.rstrip("\n")


tracemalloc.start()
contagem = Counter(logins())
em_memoria = sorted(contagem.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
_, pico_mem = tracemalloc.get_traced_memory()
tracemalloc.stop()
distintos = len(contagem)
del contagem

parts: list[int] = []
tracemalloc.start()
externo = external_top_k(logins(), 5, SPILL, n_partitions=16, on_partition=lambda i, n: parts.append(n))
_, pico_ext = tracemalloc.get_traced_memory()
tracemalloc.stop()

assert externo == em_memoria
print(f"{distintos:,} atores distintos em {sum(1 for _ in logins()):,} linhas")
print(f"Counter único: pico {pico_mem / 1e6:.1f} MB | externo (16 partições): pico {pico_ext / 1e6:.1f} MB")
print(f"chaves por partição: min {min(parts):,} / máx {max(parts):,}")
for login, n in externo:
    print(f"  {n:6,}  {login}")

# %% [markdown]
# **Pegadinhas / complexidade / como explicar em voz alta**
# - Primeira pergunta: **o que não cabe — as linhas ou as chaves distintas?** Se só as linhas não cabem, um
#   generator + `Counter` resolve (memória O(chaves)). Se as chaves não cabem, particione em disco por hash.
# - `hash()` do Python **muda a cada processo** (`PYTHONHASHSEED`): use `zlib.crc32`/`hashlib` para particionar.
# - Skew: uma chave gigante (o `github-actions[bot]`) não aumenta a memória aqui (é só um contador), mas num
#   join/ordenação ela concentraria uma partição inteira.
# - Alternativas de mercado: `sort | uniq -c | sort -rn | head` (ordenação externa do Unix), DuckDB (agrega com
#   spill automático), Spark. Algoritmos aproximados de memória fixa: Count-Min Sketch / Space-Saving (top-K
#   aproximado), HyperLogLog (contagem de distintos — o `approx_count_distinct` do Spark).
# - Complexidade: 2 leituras de I/O sequencial; memória ≈ distintos/N + K.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```sql
# -- Tudo acima é Spark SQL padrão e roda igual no Databricks SQL. Extras que só existem lá:
# -- 1) VARIANT: navegar no JSON sem schema (substitui from_json/get_json_object)
# SELECT payload:issue.labels[0].name::string FROM bronze_variant;
#
# -- 2) Range join otimizado para o point-in-time do exercício 8
# SELECT /*+ RANGE_JOIN(d, 7) */ ... ;
#
# -- 3) Funções de IA no SQL (notebook 12) — ex.: classificar o título de uma issue
# SELECT ai_classify(title, ARRAY('bug', 'feature', 'docs')) FROM silver.issues;
# ```
#
# - Em entrevista com o editor do Databricks, as mesmas soluções valem; o `display(df)` substitui o `show()`.
# - Photon acelera janelas, agregações e joins sem mudar o código — mas não UDF Python.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. `ROW_NUMBER`, `RANK` ou `DENSE_RANK` para top-N?**
# <details><summary>Resposta</summary>
# Depende de como tratar empate: exatamente N linhas → `ROW_NUMBER` com desempate determinístico; empates
# incluídos → `RANK` (pula posições); N valores distintos → `DENSE_RANK`. Exercício 3.
# </details>
#
# **2. Por que `NOT IN` devolveu zero linhas?**
# <details><summary>Resposta</summary>
# A subquery tem NULL: `x <> NULL` é desconhecido, e o `AND` de todas as comparações nunca é verdadeiro. Use
# `NOT EXISTS` ou `LEFT ANTI JOIN`. Exercício 11.
# </details>
#
# **3. Qual a diferença entre `ROWS` e `RANGE` numa janela? E o frame padrão?**
# <details><summary>Resposta</summary>
# `ROWS` conta linhas; `RANGE` usa o valor da chave de ordenação (dias faltando contam). Com `ORDER BY` e sem
# frame, o padrão é `RANGE UNBOUNDED PRECEDING` — empates entram juntos. Exercício 4.
# </details>
#
# **4. Como deduplicar mantendo o mais recente? E se houver empate no timestamp?**
# <details><summary>Resposta</summary>
# `ROW_NUMBER` por chave ordenado por timestamp desc **e** um desempate determinístico (sequência, offset, LSN);
# ou `max_by(struct, struct(ts, seq))`. Exercício 1.
# </details>
#
# **5. Explique gaps and islands.**
# <details><summary>Resposta</summary>
# Deduplica por dia, numera com `ROW_NUMBER` e subtrai do dia: valores consecutivos ficam com a mesma
# diferença, que vira a chave do grupo. Exercício 6.
# </details>
#
# **6. Como sessionizar cliques? E sessões que cruzam o limite do lote?**
# <details><summary>Resposta</summary>
# `LAG` → flag de nova sessão quando o intervalo passa do limite → soma acumulada. No limite do lote, reprocessar
# com sobreposição ou usar `session_window` em streaming. Exercício 7.
# </details>
#
# **7. Como trazer o atributo da dimensão vigente na data do fato?**
# <details><summary>Resposta</summary>
# SCD2 com `valid_from`/`valid_to` e join pelo intervalo semiaberto, ou as-of join (versão mais recente
# ≤ data do fato). Cuidado com `BETWEEN`, que duplica no dia da mudança. Exercício 8.
# </details>
#
# **8. `percentile_approx` ou exato?**
# <details><summary>Resposta</summary>
# Aproximado usa memória fixa e escala; exato guarda todos os valores do grupo. O aproximado devolve um elemento
# do conjunto (a mediana de uma quantidade par não interpola). Exercício 13.
# </details>
#
# **9. Como você diferencia o NULL do subtotal do NULL do dado num `ROLLUP`?**
# <details><summary>Resposta</summary>
# `GROUPING(col)` = 1 na linha de subtotal; `GROUPING_ID` codifica o nível. Exercício 15.
# </details>
#
# **10. Como achar o top-K de um arquivo cujas chaves não cabem na memória?**
# <details><summary>Resposta</summary>
# Particionar por hash estável em N arquivos, contar um por vez e manter um heap de K. Ou ordenação externa,
# DuckDB/Spark, ou um sketch aproximado (Count-Min/Space-Saving). Exercício 18.
# </details>
#
# **11. Seu `explode` "perdeu" linhas. Por quê?**
# <details><summary>Resposta</summary>
# `explode` descarta array vazio/nulo; use `explode_outer`. E `from_json` com JSON inválido vira NULL sem erro.
# Exercício 10.
# </details>
#
# **12. Sua consulta de janela é lenta numa tabela de 1 bilhão de linhas. O que você olha?**
# <details><summary>Resposta</summary>
# Se há `PARTITION BY` (sem ele, 1 tarefa só), skew na chave da partição (um ator com 11% dos eventos), se dá para
# agregar antes da janela, e se a janela pode virar agregação (`max_by`) — no Spark UI, a duração das tarefas
# do estágio da janela.
# </details>

# %% [markdown]
# ## Resumo
#
# - **Esclareça antes de codar:** chave, empate, NULL, limites (>, >=), fuso — cada exercício tem uma pegadinha
#   dessas.
# - **Janelas resolvem metade das perguntas:** dedup, top-N, acumulado, LAG/LEAD, ilhas, sessões, coorte.
# - **NULL é a pegadinha nº 1:** `NOT IN`, `COUNT(DISTINCT a, b)`, subtotal do `ROLLUP`, `explode`.
# - **Fale de custo:** quantos shuffles, janela sem partição, skew, exato × aproximado.
# - **Teste com o caso de borda** no exemplo pequeno — e só depois rode no dado real.

# %%
spark.stop()
