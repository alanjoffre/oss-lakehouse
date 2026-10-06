# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 07 · Gold: modelagem dimensional (Kimball), star schema e Liquid Clustering
#
# > Prova que a Gold responde perguntas de negócio com SQL simples **sem erro de grão**: o fato aponta para
# > a versão certa do repositório (join point-in-time), os agregados batem com o detalhe e a organização
# > física (Liquid Clustering) roda no Delta open source.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Arquitetura e desenvolvimento de pipelines | star schema, grão, surrogate keys, fato × agregado |
# | Databricks e processamento de dados | Liquid Clustering (`CLUSTER BY`), OPTIMIZE, Databricks SQL ☁️ |
# | Python avançado | `oss_lakehouse.gold` com testes (`tests/test_gold.py`) |
# | Times multidisciplinares | perguntas de negócio respondidas em SQL que o analista lê |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup

# %%
import shutil
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from oss_lakehouse.config import get_settings
from oss_lakehouse.gold import (
    UNKNOWN_SK,
    build_dim_actor,
    build_dim_date,
    build_dim_repo,
    build_fct_events,
    build_fct_repo_activity_daily,
    write_table,
)
from oss_lakehouse.spark import get_spark

spark = get_spark("07")
s = get_settings()
silver = spark.read.format("delta").load(s.path("silver", "gh_events"))
repo_scd2 = spark.read.format("delta").load(s.path("silver", "dim_repo_scd2"))
GOLD = {t: s.path("gold", t) for t in ("dim_date", "dim_actor", "dim_repo", "fct_events", "fct_repo_activity_daily")}
DEMO = Path(s.data_root) / "demo" / "07"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True)
print(f"silver.gh_events: {silver.count():,} eventos · silver.dim_repo_scd2: {repo_scd2.count():,} versões")

# %% [markdown]
# ## 1. Star schema e grão
#
# **O que é** — modelagem dimensional (Ralph Kimball): separar **fatos** (o que aconteceu e se mede:
# eventos, contagens) de **dimensões** (o contexto para filtrar e agrupar: quem, onde, quando). O desenho
# vira uma estrela — o fato no centro, as dimensões em volta:
#
# ```text
#                 dim_date (date_key)
#                        │
# dim_actor (actor_sk) ──┼── fct_events  [grão: 1 evento]
#                        │
#                 dim_repo (repo_sk — versão SCD2 vigente no instante do evento)
#
# fct_repo_activity_daily  [grão: 1 repositório × 1 dia]  → date_key, repo_id
# ```
#
# **Por que importa** — o analista escreve `JOIN` + `GROUP BY` simples e as métricas batem entre relatórios.
# O ponto que mais derruba modelo em produção é o **grão** (*grain*: o que UMA linha do fato representa).
# Declarar o grão antes de tudo evita somar coisas que não se somam e joins que multiplicam linhas.
#
# **Como funciona — o erro nº 1, ao vivo.** A dimensão de repositórios é SCD2 (várias linhas por `repo_id`).
# Um join só pela chave natural multiplica os eventos de repos renomeados:

# %%
ingenuo = silver.join(repo_scd2, "repo_id")
print(f"eventos na silver:                {silver.count():,}")
print(f"join só por repo_id (errado):     {ingenuo.count():,}")
print(f"eventos duplicados pelo join:     {ingenuo.count() - silver.count():,}")

# %% [markdown]
# A correção é o join **point-in-time**: o evento casa com a versão cujo intervalo
# `[valid_from, valid_to)` contém o `created_at`. É o que `build_fct_events` faz — e o grão se preserva:

# %%
dim_repo = build_dim_repo(spark, repo_scd2)
dim_actor = build_dim_actor(silver)
fct_events = build_fct_events(silver, dim_repo, dim_actor).cache()
print(f"fct_events: {fct_events.count():,} linhas · event_id distintos: "
      f"{fct_events.select('event_id').distinct().count():,} · sem versão (sk = -1): "
      f"{fct_events.filter(F.col('repo_sk') == UNKNOWN_SK).count():,}")

# %% [markdown]
# E o efeito prático: num repositório renomeado, eventos antes e depois da troca apontam para versões
# diferentes da dimensão (o relatório "por nome de repositório" mostra o nome **da época**):

# %%
REPO = 1385188237  # transferido de conta no meio do dia (notebook 05)
(fct_events.filter(F.col("repo_id") == REPO).join(dim_repo, "repo_sk")
 .groupBy("repo_sk", "repo_name").agg(F.count("*").alias("eventos"), F.min("created_at").alias("primeiro"))
 .orderBy("primeiro").show(truncate=False))

# %% [markdown]
# **Medida não aditiva** — outro erro de grão clássico: `distinct_actors` no agregado diário é "atores
# distintos **naquele repo naquele dia**". Somar entre repositórios conta a mesma pessoa várias vezes:

# %%
agg = build_fct_repo_activity_daily(silver).cache()
soma = agg.agg(F.sum("distinct_actors")).first()[0]
real = silver.select("actor_id").distinct().count()
print(f"soma de distinct_actors por repo: {soma:,}  ×  atores distintos de verdade: {real:,}")
print("eventos: soma do agregado =", f"{agg.agg(F.sum('events')).first()[0]:,}", "| silver =", f"{silver.count():,}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Primeiro declaro o grão — 'uma linha = um evento' ou 'uma linha = repo × dia'.
# > Depois escolho as dimensões e as medidas, separando aditivas (contagens, que somam em qualquer eixo) de
# > não aditivas (distintos, taxas, que precisam ser recalculadas do detalhe). O erro mais comum é join com
# > dimensão SCD2 só pela chave natural: multiplica o fato. O certo é join pela surrogate key resolvida no
# > instante do evento."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Aditiva / semiaditiva / não aditiva**: contagem soma em tudo; saldo/estoque soma entre contas mas não no
#   tempo (semiaditiva); distintos e percentuais não somam (guarde numerador e denominador, ou use *sketches*
#   como HyperLogLog — `approx_count_distinct`, ou `hll_sketch_agg`/`hll_union_agg` no Spark 3.5+).
# - **Grão mais fino possível no fato de base**: agregar depois é fácil; "desagregar" é impossível.
# - **Fato sem fato** (*factless fact*): registra só a ocorrência (ex.: "ator X estrelou repo Y") — a medida é
#   a contagem de linhas.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Star schema exige ETL e governança de dimensões; para exploração ad hoc de dados crus, a Silver basta.
# - Join point-in-time é um *range join* — caro em escala (ver Databricks ☁️ abaixo: hint de range join).

# %% [markdown]
# ## 2. Chaves e dimensões: surrogate × natural, conformadas, membro desconhecido
#
# **O que é**
# - **Chave natural** (*natural/business key*): o identificador da fonte (`repo_id`, `actor_id`).
# - **Surrogate key** (chave substituta): identificador do **data warehouse**, sem significado de negócio
#   (`repo_sk`). Existe porque a SCD2 tem várias linhas por chave natural — cada versão precisa de chave própria.
# - **Dimensão conformada** (*conformed dimension*): a MESMA dimensão (mesmas chaves e atributos) usada por
#   vários fatos — `dim_date` e `dim_repo` servem tanto ao `fct_events` quanto ao agregado diário. É o que
#   permite comparar métricas de fatos diferentes ("*drill across*").
# - **Membro desconhecido** (`sk = -1`): linha da dimensão para fato sem correspondência — FK nunca nula,
#   join interno nunca perde fato em silêncio.
#
# **Por que importa** — sem surrogate key não há SCD2; sem dimensão conformada cada time define "repositório"
# de um jeito e os números não batem na reunião.
#
# **Como funciona**

# %%
dim_date = build_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31))
print("dim_date:", dim_date.count(), "dias")
dim_date.filter("date BETWEEN '2026-09-30' AND '2026-10-04'").show()
dim_actor.orderBy(F.desc("is_bot"), "actor_id").show(3, truncate=False)
dim_repo.filter((F.col("repo_id") == REPO) | (F.col("repo_sk") == UNKNOWN_SK)).orderBy("valid_from").show(truncate=False)

# %% [markdown]
# Escolhas deste modelo (e por quê):
#
# | Dimensão | Tipo | Chave | Motivo |
# |---|---|---|---|
# | `dim_date` | gerada (não vem da fonte) | `date_key` int `yyyymmdd` | legível, ordenável, filtros por mês/dia da semana sem função de data |
# | `dim_actor` | SCD1 | `actor_sk` = xxhash64(actor_id) | login também muda, mas ninguém pediu histórico — vale o atual |
# | `dim_repo` | SCD2 | `repo_sk` da SCD2 (hash de repo_id + valid_from) | nome/dono no tempo é pergunta de negócio |
#
# > 🎤 **Resposta de 30 s:** "Surrogate key existe porque a chave natural não identifica uma versão: com
# > SCD2, o mesmo `repo_id` tem várias linhas. Eu gero a sk de forma determinística — hash da chave natural com
# > o início da vigência — para que reconstruir a dimensão não mude as chaves já gravadas nos fatos. E tenho
# > membro desconhecido, para nenhum fato sumir num inner join."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Chave natural "inteligente"** (código com significado) muda quando o negócio muda — mais um motivo para sk.
# - **Durable key**: `repo_id` é estável entre versões — o agregado diário usa ele (grão repo × dia, não
#   versão × dia) e junta com `dim_repo WHERE is_current` para mostrar o nome atual.
# - **Role-playing dimension**: a mesma `dim_date` usada como "data de abertura" e "data de merge" num fato
#   acumulativo de PR (seção 3).
# - **Junk dimension**: flags de baixa cardinalidade (`event_type`, `action`) poderiam ir para uma dimensão
#   pequena com todas as combinações; aqui ficam como **dimensões degeneradas** no fato (atributo sem tabela).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Hash como sk: 8 bytes, colisão possível (desprezível aqui); sequência/identity é menor mas não determinística.
# - Dimensão de data gerada para um intervalo fixo precisa de manutenção (estender o ano).

# %% [markdown]
# ## 3. Tipos de fato: transacional, snapshot periódico, snapshot acumulativo
#
# **O que é**
#
# | Tipo | Grão | Exemplo aqui | Atualização |
# |---|---|---|---|
# | Transacional | 1 evento | `fct_events` | só insere |
# | Snapshot periódico | 1 entidade × 1 período | `fct_repo_activity_daily` (repo × dia) | 1 linha nova por período |
# | Snapshot acumulativo | 1 processo com marcos | ciclo de vida do PR: aberto → revisado → mergeado | a MESMA linha é atualizada a cada marco |
#
# **Por que importa** — cada pergunta tem o seu fato: "quantos eventos" (transacional), "como evoluiu a
# atividade por dia" (periódico), "quanto tempo um PR leva do abrir ao merge" (acumulativo).
#
# **Como funciona** — o acumulativo de PR, montado da Silver (só PRs que **abriram** nestas 3 horas):

# %%
pr_ev = silver.filter("pr_number IS NOT NULL AND event_type IN ('PullRequestEvent', 'PullRequestReviewEvent')")
marco = lambda cond: F.min(F.when(F.expr(cond), F.col("created_at")))  # noqa: E731
fct_pr_acumulativo = (
    pr_ev.groupBy("repo_id", "pr_number")
    .agg(marco("event_type = 'PullRequestEvent' AND action = 'opened'").alias("aberto_em"),
         marco("event_type = 'PullRequestReviewEvent'").alias("primeira_revisao_em"),
         marco("event_type = 'PullRequestEvent' AND action = 'merged'").alias("mergeado_em"))
    .filter("aberto_em IS NOT NULL")
    .withColumn("min_ate_merge", F.round((F.unix_timestamp("mergeado_em") - F.unix_timestamp("aberto_em")) / 60, 1))
)
fct_pr_acumulativo.agg(
    F.count("*").alias("prs_abertos"),
    F.count("primeira_revisao_em").alias("com_revisao"),
    F.count("mergeado_em").alias("mergeados_na_janela"),
    F.percentile_approx("min_ate_merge", 0.5).alias("mediana_min_ate_merge"),
).show()

# %% [markdown]
# Repare na janela: com só 3 horas de dado, a maioria dos PRs abertos ainda não tem os marcos seguintes — é
# exatamente o caso de uso do acumulativo: a linha nasce com `mergeado_em` nulo e é **atualizada** (MERGE)
# quando o marco chega.
#
# > 🎤 **Resposta de 30 s:** "Transacional guarda cada evento e só cresce. Snapshot periódico tira uma foto
# > por período — bom para tendência e para medidas semiaditivas. Acumulativo é uma linha por processo, com
# > uma coluna de data por marco, atualizada via MERGE — ideal para medir tempo entre etapas, como lead time de PR."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - O agregado diário aqui é tecnicamente um **agregado** do transacional (só há linha para repo com atividade);
#   um snapshot periódico "puro" teria linha para todo repo existente, mesmo sem atividade (ex.: estrelas acumuladas).
# - Acumulativo exige MERGE e chave estável do processo (`repo_id` + `pr_number`).
# - O formato atual do evento de PR não traz `created_at` do PR — o `aberto_em` é o instante do evento `opened`.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Acumulativo com processo sem fim definido (issue que nunca fecha) acumula linhas "abertas" para sempre — ok,
#   mas o MERGE precisa tocar só as abertas.
# - Snapshot periódico de entidade grande × período curto explode volume (1 bi repos × dia).

# %% [markdown]
# ## 4. Gravando a Gold — e Liquid Clustering no Delta open source
#
# **O que é** — *Liquid Clustering* (`CLUSTER BY`) organiza os dados dentro dos arquivos pelas colunas
# escolhidas (curva de Hilbert), substituindo particionamento + Z-order. O `OPTIMIZE` reorganiza
# **incrementalmente** (só o que ainda não está clusterizado) e as colunas podem mudar com `ALTER TABLE` sem
# reescrever a tabela.
#
# **Por que importa** — partição fixa por coluna de alta cardinalidade gera milhares de arquivos pequenos;
# partição por data com pouco dado por dia também. Liquid dá *data skipping* (pular arquivos pelo min/max)
# sem a rigidez da partição.
#
# **Como funciona** — testado aqui no Delta 4.4 OSS. Primeiro, a armadilha: `DataFrameWriter.clusterBy(...)`
# com `save(caminho)` **é ignorado em silêncio**:

# %%
armadilha = str(DEMO / "armadilha_clusterby")
fct_events.limit(1000).write.format("delta").clusterBy("date_key", "repo_id").save(armadilha)
print("clusteringColumns via DataFrameWriter.clusterBy:",
      DeltaTable.forPath(spark, armadilha).detail().first()["clusteringColumns"])

# %% [markdown]
# Por isso `write_table` usa SQL — `CREATE OR REPLACE TABLE delta.\`caminho\` CLUSTER BY (...) AS SELECT …`:

# %%
write_table(spark, dim_date, GOLD["dim_date"])
write_table(spark, dim_actor, GOLD["dim_actor"])
write_table(spark, dim_repo, GOLD["dim_repo"], cluster_by=["repo_id"])
write_table(spark, fct_events, GOLD["fct_events"], cluster_by=["date_key", "repo_id"])
write_table(spark, agg, GOLD["fct_repo_activity_daily"], cluster_by=["date_key", "repo_id"])

for nome, caminho in GOLD.items():
    d = DeltaTable.forPath(spark, caminho).detail().first()
    print(f"gold.{nome:<24} arquivos={d['numFiles']:>2}  {d['sizeInBytes'] / 1e6:6.1f} MB  "
          f"partição={list(d['partitionColumns'])}  clustering={list(d['clusteringColumns'])}")

# %%
opt = spark.sql(f"OPTIMIZE delta.`{GOLD['fct_events']}`").first()["metrics"]
print("OPTIMIZE fct_events → arquivos adicionados:", opt["numFilesAdded"], "removidos:", opt["numFilesRemoved"])
fct = spark.read.format("delta").load(GOLD["fct_events"])
print("arquivos:", len(fct.inputFiles()), "| lidos para repo_id = 1385188237:",
      len(fct.filter(F.col("repo_id") == REPO).inputFiles()))

# %% [markdown]
# Leitura honesta: o fato de 3 horas tem ~7 MB e, depois do `OPTIMIZE`, cabe em **1 arquivo** — não há o que
# pular, então aqui o filtro por `repo_id` lê o arquivo inteiro. O *data skipping* só aparece com volume
# (vários arquivos com faixas de min/max diferentes). O que esta seção prova é que o recurso **existe e
# funciona** no Delta OSS: a tabela nasce com `clusteringColumns` e o `OPTIMIZE` reorganiza. O ganho de leitura
# está medido no notebook 09 (§7), com o dia inteiro e arquivos limitados a 16 MB.
#
# **Particionar ou não a Gold?** Regra prática: só particione quando cada partição tiver ~1 GB ou mais e as
# consultas filtrarem SEMPRE pela coluna. Aqui 3 horas de eventos ocupam ~7 MB no fato — particionar por
# data criaria arquivos pequenos sem ganho. Liquid Clustering por `date_key, repo_id` dá a poda sem a rigidez.
#
# > 🎤 **Resposta de 30 s:** "Na Gold eu não particiono tabela pequena ou média: partição boa tem gigabytes.
# > Uso Liquid Clustering nas colunas de filtro e join — aqui `date_key` e `repo_id` — e OPTIMIZE periódico.
# > Testei no Delta OSS: `CLUSTER BY` em SQL funciona, mas o `clusterBy` do DataFrameWriter com save por
# > caminho é ignorado sem erro, então crio a tabela por SQL."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Liquid × partição + Z-order**: Z-order não é incremental (reescreve a partição toda) e as colunas de
#   partição não mudam sem reescrever a tabela. Liquid é incremental e as chaves mudam com `ALTER TABLE … CLUSTER BY`.
# - **Quantas colunas?** Até 4 no Delta; mais colunas = menos poda em cada uma. Escolha as de filtro mais seletivo.
# - **Liquid é incompatível com `PARTITIONED BY`** e com Z-order na mesma tabela.
# - **Clustering on write**: no OSS a clusterização acontece no `OPTIMIZE`; escritas pequenas ficam "não
#   clusterizadas" até ele rodar.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Tabela pequena (< 1 GB): clustering não paga o OPTIMIZE — mantenha simples.
# - Leitores antigos do Delta (sem a *table feature* `clustering`) não leem a tabela.

# %% [markdown]
# ## 5. Perguntas de negócio em SQL sobre a Gold
#
# O que o analista escreve — joins pela surrogate key e `GROUP BY`, sem saber nada de JSON ou de SCD2.

# %%
for nome, caminho in GOLD.items():
    spark.read.format("delta").load(caminho).createOrReplaceTempView(nome)

# %% [markdown]
# **Quais repositórios tiveram mais atividade humana (atores distintos, sem bots)?**

# %%
spark.sql("""
SELECT r.repo_name, COUNT(DISTINCT f.actor_sk) AS atores_humanos, COUNT(*) AS eventos
FROM fct_events f
JOIN dim_actor a ON a.actor_sk = f.actor_sk AND NOT a.is_bot
JOIN dim_repo  r ON r.repo_sk  = f.repo_sk
GROUP BY r.repo_name
ORDER BY atores_humanos DESC, eventos DESC
LIMIT 8
""").show(truncate=False)

# %% [markdown]
# **Qual a participação de bots, no total e por tipo de evento?**

# %%
spark.sql("""
SELECT f.event_type,
       COUNT(*) AS eventos,
       ROUND(100 * AVG(CAST(a.is_bot AS INT)), 1) AS pct_bot
FROM fct_events f JOIN dim_actor a ON a.actor_sk = f.actor_sk
GROUP BY ROLLUP (f.event_type)
ORDER BY eventos DESC
LIMIT 8
""").show(truncate=False)

# %% [markdown]
# (A linha com `event_type` nulo é o total do `ROLLUP`.)
#
# **Qual a taxa de merge de PRs?** Fechamentos com merge ÷ todos os fechamentos. Premissa (inferida do dado,
# não de documentação): no formato atual do evento não existe `pull_request.merged`, e `action` traz `merged`
# e `closed` como valores distintos (notebook 05) — lemos `closed` como "fechado sem merge". Se a premissa
# cair, a taxa muda; por isso ela fica escrita aqui. Taxa é **não aditiva**: soma numerador e denominador,
# divide no fim.

# %%
spark.sql("""
SELECT SUM(prs_opened) AS abertos, SUM(prs_merged) AS mergeados, SUM(prs_closed_unmerged) AS fechados_sem_merge,
       ROUND(100 * SUM(prs_merged) / NULLIF(SUM(prs_merged) + SUM(prs_closed_unmerged), 0), 1) AS taxa_merge_pct
FROM fct_repo_activity_daily
""").show()

# %% [markdown]
# **Como a atividade se distribui por hora (UTC), humanos × bots?**

# %%
spark.sql("""
SELECT f.event_hour AS hora_utc,
       SUM(CASE WHEN a.is_bot THEN 0 ELSE 1 END) AS eventos_humanos,
       SUM(CASE WHEN a.is_bot THEN 1 ELSE 0 END) AS eventos_bots,
       COUNT(DISTINCT f.repo_sk) AS repos_ativos
FROM fct_events f JOIN dim_actor a ON a.actor_sk = f.actor_sk
GROUP BY f.event_hour ORDER BY hora_utc
""").show()

# %% [markdown]
# **Consistência entre os dois fatos (drill across pela dimensão conformada):** o agregado diário tem de bater
# com o detalhe.

# %%
spark.sql("""
SELECT d.date, d.day_name,
       (SELECT COUNT(*) FROM fct_events f WHERE f.date_key = d.date_key)            AS eventos_no_detalhe,
       (SELECT SUM(events) FROM fct_repo_activity_daily g WHERE g.date_key = d.date_key) AS eventos_no_agregado
FROM dim_date d WHERE d.date_key = 20261001
""").show()

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Com o star schema, cada pergunta é um join pela surrogate key e um GROUP BY. Taxa
# > eu calculo como razão de somas, nunca média de taxas. E valido o modelo cruzando os fatos pela dimensão
# > conformada: o agregado diário tem que bater com o detalhe."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Média de taxas × razão de somas**: média das taxas de merge por repo dá o mesmo peso a um repo com 1 PR e
#   a um com 500 — quase sempre errado.
# - **`ROLLUP`/`CUBE`/`GROUPING SETS`**: subtotais numa consulta só.
# - **Data quality da Gold**: teste de reconciliação (soma do agregado = contagem do detalhe) roda no pipeline,
#   não só no notebook — ver notebook 08.
# </details>

# %% [markdown]
# ## 6. OBT (*one big table*) × star schema
#
# **O que é** — OBT é a tabela "larga" já com todas as dimensões juntadas no fato: zero joins para consultar.
#
# **Por que importa** — ferramentas de BI e motores colunares lidam bem com tabela larga (lê só as colunas
# usadas, compressão por dicionário torna barata a repetição). Muitos times modernos servem OBT para
# dashboards e mantêm o star como camada de integração.
#
# **Como funciona**

# %%
obt = (fct_events.join(dim_actor.select("actor_sk", "actor_login", "is_bot"), "actor_sk")
       .join(dim_repo.select("repo_sk", "repo_name", "repo_owner"), "repo_sk")
       .join(dim_date.select("date_key", "date", "day_name", "month_name"), "date_key"))
obt_path = str(DEMO / "obt_events")
obt.write.format("delta").save(obt_path)
star_mb = sum(DeltaTable.forPath(spark, p).detail().first()["sizeInBytes"] for p in GOLD.values()) / 1e6
obt_mb = DeltaTable.forPath(spark, obt_path).detail().first()["sizeInBytes"] / 1e6
print(f"OBT: {len(obt.columns)} colunas, {obt_mb:.1f} MB  ×  star (5 tabelas): {star_mb:.1f} MB")

# %% [markdown]
# Resultado contraintuitivo: aqui a OBT **não** ficou maior que o star (≈14 MB × ≈18 MB). Dois motivos:
# ela carrega só 3 atributos de cada dimensão, enquanto o star guarda as dimensões inteiras (só a `dim_repo`
# com as vigências passa de 7 MB); e em Parquet o valor repetido custa pouco (dicionário + RLE). Não
# generalize: com dimensões largas (dezenas de atributos de texto) repetidas em bilhões de linhas, a OBT
# cresce — e o custo que importa nem é o disco, é **reescrever** a OBT quando um atributo muda.
#
# > 🎤 **Resposta de 30 s:** "Star é o modelo de integração: dimensões conformadas, SCD2, uma fonte de verdade
# > por entidade. OBT é o modelo de servir: zero joins, ótimo para dashboard e para colunar, ao custo de
# > redundância e de reprocessar a tabela quando uma dimensão muda. Faço OBT derivada do star, não no lugar dele."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - OBT com SCD1 "congela" o atributo da época da carga; mudar o nome de um repo exige reescrever a OBT.
# - **Snowflake schema** (dimensões normalizadas em subdimensões) economiza espaço e complica consultas —
#   raramente compensa em armazenamento colunar barato.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - OBT para tudo vira "uma tabela por dashboard" e métricas divergentes. Mantenha a definição de métrica num lugar só.

# %% [markdown]
# ## 7. Data Vault — conceito e quando faz sentido
#
# **O que é** — modelagem para a camada de **integração** de muitas fontes, com histórico completo e
# carga paralela:
#
# ```text
# HUB  (só a chave de negócio)      hub_repo(repo_hk, repo_id, load_ts, source)
#                                    hub_actor(actor_hk, actor_id, …)
# LINK (relação entre hubs)         link_event(event_hk, actor_hk, repo_hk, …)
# SATELLITE (atributos + histórico) sat_repo_name(repo_hk, load_ts, hash_diff, repo_name, repo_owner)
# ```
#
# **Por que importa** — fonte nova vira hub/link/satellite novos, sem remodelar o que existe; tudo é
# insert-only (auditável); hubs, links e satélites carregam em paralelo (chaves por hash).
#
# **Quando faz sentido** — dezenas de sistemas de origem, requisitos fortes de auditoria (banco, seguro,
# governo), modelo de negócio que muda muito. **Quando não** — poucas fontes e foco em consumo analítico: o
# Vault exige uma camada dimensional por cima (mais joins, mais tabelas). Num lakehouse, é comum: Bronze →
# (Vault como Silver de integração) → Gold dimensional.
#
# > 🎤 **Resposta de 30 s:** "Data Vault separa chave (hub), relação (link) e atributo com histórico
# > (satellite), tudo insert-only com hash keys. Brilha na integração de muitas fontes com auditoria; não é
# > modelo de consumo — por cima dele eu ainda sirvo um star schema."

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```sql
# -- Unity Catalog: tabela gerenciada com Liquid Clustering automático (o Databricks escolhe as colunas
# -- pelo padrão de consulta) e Predictive Optimization rodando OPTIMIZE/VACUUM sozinho.
# CREATE OR REPLACE TABLE oss.gold.fct_events CLUSTER BY AUTO AS SELECT * FROM oss.silver.fct_events_staging;
#
# -- Chaves primária/estrangeira são INFORMATIVAS (não validadas); com RELY o otimizador pode usá-las
# -- (ex.: eliminar join desnecessário).
# ALTER TABLE oss.gold.dim_repo ADD CONSTRAINT pk_dim_repo PRIMARY KEY (repo_sk) RELY;
# ALTER TABLE oss.gold.fct_events ADD CONSTRAINT fk_repo FOREIGN KEY (repo_sk) REFERENCES oss.gold.dim_repo;
#
# -- Range join (point-in-time) grande: dica de bin size para a otimização de range join.
# SELECT /*+ RANGE_JOIN(r, 3600) */ … FROM events e JOIN dim_repo r ON e.repo_id = r.repo_id
#   AND e.created_at BETWEEN r.valid_from AND coalesce(r.valid_to, '9999-12-31');
# ```
#
# - **Materialized views** no Databricks SQL / Lakeflow Declarative Pipelines: o agregado diário vira uma MV com
#   refresh **incremental** quando possível — em vez do `CREATE OR REPLACE` completo usado aqui.
# - **Identity columns** para sk sequencial (`GENERATED ALWAYS AS IDENTITY`) — cuidado com rebuild.
# - **Metric views** do Unity Catalog: a definição da métrica (ex.: taxa de merge = razão de somas) fica
#   registrada uma vez e é reutilizada por dashboards (AI/BI) e pelo Genie — resolve "cada dashboard calcula de um jeito".
# - **Power BI** lê a Gold pelo SQL Warehouse (DirectQuery ou Import); star schema é o formato que o modelo
#   semântico do Power BI espera.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. O que é o grão de uma tabela fato e por que é a primeira decisão?**
# <details><summary>Resposta</summary>
# É o que uma linha representa ("um evento", "um repo por dia"). Define quais dimensões cabem, quais medidas
# somam e protege contra joins que multiplicam linhas. Aqui, o join só por `repo_id` com a SCD2 inflou o fato.
# </details>
#
# **2. Surrogate key × natural key?**
# <details><summary>Resposta</summary>
# Natural vem da fonte; surrogate é do DW, sem significado, e identifica uma VERSÃO (necessária na SCD2). Gero
# por hash determinístico de (chave natural, valid_from) para rebuild não mudar as chaves.
# </details>
#
# **3. Como você liga o fato à versão certa de uma dimensão SCD2?**
# <details><summary>Resposta</summary>
# Join point-in-time: mesma chave natural e `valid_from <= ts < valid_to`, resolvido na carga do fato, que grava
# a sk. Consultas depois usam só a sk (join de igualdade, barato).
# </details>
#
# **4. Transacional, snapshot periódico e acumulativo — dê um exemplo de cada.**
# <details><summary>Resposta</summary>
# Evento do GitHub; atividade por repo × dia; ciclo de vida do PR (aberto/revisado/mergeado na mesma linha,
# atualizada por MERGE).
# </details>
#
# **5. O que é dimensão conformada?**
# <details><summary>Resposta</summary>
# Mesma dimensão compartilhada por vários fatos (dim_date, dim_repo), permitindo comparar e cruzar métricas
# ("drill across") sem divergência de definição.
# </details>
#
# **6. Medida aditiva × não aditiva — como você trata atores distintos e taxas?**
# <details><summary>Resposta</summary>
# Não somar entre grãos: recalcular do detalhe, guardar numerador/denominador, ou sketches (HLL). Demonstrado:
# soma de distintos por repo é muito maior que os distintos reais.
# </details>
#
# **7. OBT ou star schema?**
# <details><summary>Resposta</summary>
# Star para integração e governança; OBT derivada para servir BI com zero joins. OBT sozinha duplica lógica e
# precisa ser reescrita quando dimensão muda.
# </details>
#
# **8. Particionaria a Gold? Por quê?**
# <details><summary>Resposta</summary>
# Só com partições de ~1 GB+ e filtro sempre pela coluna. Caso contrário, Liquid Clustering nas colunas de
# filtro/join e OPTIMIZE. Aqui, o fato de 3 horas tem ~7 MB — não particiono.
# </details>
#
# **9. Liquid Clustering × Z-order?**
# <details><summary>Resposta</summary>
# Liquid é incremental, as colunas mudam sem reescrever, dispensa partição. Z-order reescreve a partição toda e
# depende do esquema de partição. Liquid roda no Delta OSS (testado); `clusterBy` do DataFrameWriter por caminho é ignorado.
# </details>
#
# **10. Quando Data Vault?**
# <details><summary>Resposta</summary>
# Muitas fontes, auditoria forte, modelo que muda — como camada de integração. Por cima, ainda um dimensional
# para consumo.
# </details>

# %% [markdown]
# ## Resumo
#
# - Grão primeiro; o erro nº 1 é join com SCD2 só pela chave natural (multiplica o fato) — corrigido com join point-in-time.
# - Surrogate key determinística (hash) + membro desconhecido (-1) + dimensões conformadas.
# - Três tipos de fato: transacional (`fct_events`), periódico/agregado (`fct_repo_activity_daily`), acumulativo (PR).
# - Medidas não aditivas (distintos, taxas) = recalcular do detalhe ou razão de somas.
# - Gold sem partição, com Liquid Clustering via SQL (`CLUSTER BY` funciona no Delta OSS; `DataFrameWriter.clusterBy` por caminho é ignorado).

# %%
spark.stop()
