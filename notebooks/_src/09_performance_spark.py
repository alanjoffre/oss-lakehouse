# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 09 · Performance no Spark
#
# > Prova, com plano de execução e tempo medido sobre um dia inteiro do GH Archive (~2 milhões
# > de eventos), o que deixa um job Spark lento — shuffle, skew, small files, layout ruim,
# > UDF, spill — e o que conserta cada caso.
#
# | Requisito da vaga | Onde aparece aqui |
# |---|---|
# | Databricks e processamento de dados | §1–§13: plano físico, AQE, joins, skew, layout Delta |
# | Python avançado | §11 (UDF Python × nativa × pandas UDF), `oss_lakehouse.perf` (context managers, dataclasses) |
# | Arquitetura de pipelines | §6–§8: small files, data skipping, particionamento × Z-order × Liquid |
# | Microsoft Azure | ☁️ dimensionamento de cluster e tipos de VM na Azure |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Como ler os tempos.** Tudo roda em `local[4]` (4 núcleos, 1 JVM, driver de 3 GB) num
# laptop **compartilhado com outros processos**. Por isso cada medição roda 3 vezes (com 1
# execução de aquecimento) e reportamos a **mediana**. As saídas gravadas aqui foram medidas
# com a máquina **sob carga pesada** (outras suítes rodando ao mesmo tempo): os segundos mudam
# de uma execução para outra e diferenças pequenas entre variantes (menos de ~2×) podem sumir
# ou se inverter. Por isso o texto **não cita segundos** — aponta para a tabela da célula — e
# cada seção se apoia primeiro numa evidência que **não depende do relógio**: o plano, o número
# de tasks, os registros por task, os arquivos lidos, os bytes de shuffle e de spill. Quando a
# medição local contradiz a teoria, o texto diz.

# %% [markdown]
# ## Setup

# %%
import glob
import os
import shutil

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from oss_lakehouse.bronze import GH_EVENT_SCHEMA
from oss_lakehouse.config import get_settings
from oss_lakehouse.perf import (
    SparkUI,
    as_table,
    compare,
    delta_active_files,
    files_read,
    grep_plan,
    job_group,
    salted_join,
    spark_conf,
    stopwatch,
    time_it,
)
from oss_lakehouse.spark import get_spark

spark = get_spark(
    "09",
    **{
        "spark.driver.memory": "3g",
        # Baixo DE PROPÓSITO (padrão: 1g) para o §12 provocar o erro sem derrubar o driver.
        "spark.driver.maxResultSize": "128m",
    },
)
ui = SparkUI.of(spark)
s = get_settings()
D = s.path("demo", "09")  # tudo deste notebook (destrutivo) fica em data/demo/09
print(spark.version, "|", spark.sparkContext.master, "| Spark UI:", spark.sparkContext.uiWebUrl)
print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"),
      "| AQE =", spark.conf.get("spark.sql.adaptive.enabled"))


def table_exists(path: str) -> bool:
    return os.path.isdir(os.path.join(path, "_delta_log"))


def build_once(path: str, write) -> None:
    """Monta a tabela só se ainda não existir (o notebook roda de novo em segundos)."""
    if table_exists(path):
        print(f"reaproveitada: {path.split('/data/')[-1]}")
        return
    with stopwatch() as sw:
        write(path)
    print(f"construída agora em {sw.seconds:.0f}s: {path.split('/data/')[-1]}")


def detail(path: str) -> dict:
    r = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").first()
    return {"arquivos": r.numFiles, "MB": round(r.sizeInBytes / 1e6, 1)}


# %% [markdown]
# **Base do notebook: `events_day`.** O dia 2026-10-01 inteiro (24 arquivos horários: 21 do
# `raw_cache` + 3 da landing), achatado em colunas e gravado uma vez em Delta. Guardamos o
# `payload` (JSON bruto, a coluna mais pesada) de propósito: ele torna visível o efeito de
# column pruning e de spill.
#
# Detalhe de layout: por padrão o Spark **empacota** vários `.json.gz` pequenos numa task só
# (`spark.sql.files.maxPartitionBytes` = 128 MB) e sairiam ~5 arquivos. Baixando para 16 MB,
# cada arquivo horário (não divisível, porque gzip não é *splittable*) vira uma task e um
# arquivo Delta → 24 arquivos, cada um com ~1 hora de dados. Isso importa no §7.

# %%
EVENTS = f"{D}/events_day"


def write_events_day(path: str) -> None:
    root = s.data_root
    files = sorted(glob.glob(f"{root}/raw_cache/gharchive/*.json.gz"))
    names = {os.path.basename(f) for f in files}
    files += [f for f in sorted(glob.glob(f"{root}/landing/gharchive/*.json.gz"))
              if os.path.basename(f) not in names]
    assert len(files) == 24, len(files)
    with spark_conf(spark, {"spark.sql.files.maxPartitionBytes": str(16 * 1024 * 1024)}):
        (spark.read.schema(GH_EVENT_SCHEMA).json(files)
         .select("id", "type",
                 F.col("actor.id").alias("actor_id"), F.col("actor.login").alias("actor_login"),
                 F.col("repo.id").alias("repo_id"), F.col("repo.name").alias("repo_name"),
                 F.col("org.login").alias("org_login"),
                 F.to_timestamp("created_at").alias("created_at"), "payload")
         .withColumn("event_hour", F.hour("created_at"))
         .write.format("delta").mode("overwrite").save(path))


build_once(EVENTS, write_events_day)


def ev():
    """DataFrame NOVO a cada chamada (ver a nota sobre reuso de shuffle no §3)."""
    return spark.read.format("delta").load(EVENTS)


N_EVENTS = ev().count()
print(f"{N_EVENTS:,} eventos | {detail(EVENTS)}")
ev().drop("payload").show(3, truncate=30)

# %% [markdown]
# Tabelas auxiliares (também montadas uma vez): `actors` (1 linha por `actor_login`, ~450 mil)
# e `top_repos` (repositórios com ≥ 100 eventos no dia — uma dimensão pequena).

# %%
ACTORS, TOP_REPOS = f"{D}/actors", f"{D}/top_repos"
build_once(ACTORS, lambda p: ev().groupBy("actor_login")
           .agg(F.min("created_at").alias("first_seen"), F.count("*").alias("n_events"))
           .write.format("delta").mode("overwrite").save(p))
build_once(TOP_REPOS, lambda p: ev().groupBy("repo_id").agg(F.count("*").alias("n"))
           .filter("n >= 100").write.format("delta").mode("overwrite").save(p))
actors = lambda: spark.read.format("delta").load(ACTORS)  # noqa: E731
top_repos = lambda: spark.read.format("delta").load(TOP_REPOS)  # noqa: E731
print(f"actors: {actors().count():,} linhas {detail(ACTORS)} | top_repos: {top_repos().count():,} linhas")

# %% [markdown]
# ## 1. Lazy evaluation, jobs, stages e tasks
#
# **O que é.** *Transformações* (`filter`, `select`, `groupBy`…) só descrevem o cálculo e
# devolvem outro DataFrame na hora — é a **lazy evaluation** (avaliação preguiçosa). Só uma
# *action* (`count`, `collect`, `write`) dispara execução. Cada action vira um ou mais
# **jobs**; cada job é cortado em **stages** nas fronteiras de shuffle; cada stage roda uma
# **task** por partição.
#
# **Por que importa.** Lazy permite ao otimizador (Catalyst) ver o plano inteiro antes de
# rodar: empurrar filtros para a leitura, ler só as colunas usadas, escolher o join. E
# explica um erro comum: "a célula do `filter` rodou em 0 s" — não rodou nada.
#
# **Como funciona.** Transformações **narrow** (estreitas: `filter`, `select`, `withColumn`)
# — cada partição de saída depende de uma partição de entrada, então ficam no mesmo stage,
# encadeadas em memória (*pipelining*). Transformações **wide** (largas: `groupBy`, `join`,
# `distinct`, `orderBy`) precisam reunir as linhas da mesma chave: exigem **shuffle**
# (redistribuição pela rede/disco) e abrem um stage novo.
#
# ```text
# Stage 1 (narrow, 1 task por arquivo)          Stage 2 (1 task por partição de shuffle)
# scan → filter → select → agregação parcial  ══ Exchange (shuffle) ══►  agregação final
# ```

# %%
with stopwatch() as sw_t:
    narrow = ev().filter("type = 'PushEvent'").select("actor_login", "repo_id")
    wide = narrow.groupBy("repo_id").count()
print(f"montar as transformações: {sw_t.seconds * 1000:.1f} ms (nada executou)")

with job_group(spark, "s1-narrow"), stopwatch() as sw_n:
    n_push = narrow.count()
with job_group(spark, "s1-wide"), stopwatch() as sw_w:
    n_repos = wide.count()
print(f"action narrow (count): {sw_n.seconds:.2f}s → {n_push:,} pushes")
print(f"action wide (groupBy+count): {sw_w.seconds:.2f}s → {n_repos:,} repositórios com push\n")
for g in ("s1-narrow", "s1-wide"):
    print(g, f"— {len(ui.jobs(g))} job(s)")
    print(as_table([st.as_row() for st in ui.stage_summaries(g)]), "\n")

# %% [markdown]
# Leitura da tabela acima: o stage com **50 tasks** e quase nenhum tempo é do **próprio Delta**
# reconstruindo o estado da tabela a partir do `_delta_log` (*log replay*;
# `spark.databricks.delta.snapshotPartitions` = 50). O stage de scan tem uma task por bloco de
# arquivos. O `count()` do narrow tem um shuffle minúsculo (soma de contagens parciais), o wide
# mostra o shuffle de verdade: bytes escritos no stage do scan = bytes lidos no stage seguinte.
#
# O plano físico (`explain("formatted")`) mostra a mesma coisa: `Exchange` é a fronteira de stage.

# %%
wide.explain("formatted")

# %% [markdown]
# Como ler: de baixo para cima. `Scan parquet` com **ReadSchema** só das colunas usadas e
# **PushedFilters** com o `type = PushEvent` (§9); `HashAggregate(partial_count)` *antes* do
# `Exchange hashpartitioning(repo_id, N)` (agregação parcial — cada task já soma o que tem
# antes de mandar pela rede); depois `HashAggregate(count)` final. `AdaptiveSparkPlan
# isFinalPlan=false`: o plano ainda pode mudar em execução (AQE, §3).
#
# > 🎤 **Resposta de 30 s:** "Spark é lazy: transformação monta um plano, action executa. O
# > plano vira jobs, os jobs se dividem em stages em cada shuffle — que aparece como `Exchange`
# > no plano — e cada stage roda uma task por partição. Transformação narrow fica no mesmo stage;
# > wide (groupBy, join, distinct) exige shuffle e é onde mora o custo: disco, rede e serialização."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que o shuffle é caro?** Cada task do stage anterior escreve arquivos de shuffle
#   (ordenados por partição de destino) no disco local; cada task do stage seguinte busca o
#   seu pedaço de **todas** as tasks anteriores (N×M blocos) pela rede. Serializa, comprime,
#   grava, transfere, descomprime.
# - **Whole-stage codegen** (`*(n)` no plano): o Spark gera bytecode Java para um stage inteiro
#   de operadores narrow, sem chamada virtual por linha. UDF Python quebra isso (§11).
# - **Uma action pode gerar vários jobs**: leitura de metadados, broadcast, o próprio Delta
#   (log replay, estatísticas). Por isso medimos por *job group* (`oss_lakehouse.perf.job_group`).
# - O Catalyst tem 4 fases: análise → otimização lógica (regras: pushdown, pruning, constant
#   folding) → planejamento físico (escolha de join por custo/tamanho) → codegen.
# </details>
#
# **Trade-offs / quando NÃO usar:** lazy cobra um preço — reusar o mesmo DataFrame em várias
# actions recalcula tudo a cada vez (a não ser que o shuffle seja reaproveitado ou haja cache,
# §10); e o erro de dado só aparece na action, longe da linha que o causou.

# %% [markdown]
# ## 2. `spark.sql.shuffle.partitions`: quantas tasks depois do shuffle
#
# **O que é.** O número de partições (e de tasks) do lado de leitura de todo shuffle de
# DataFrame/SQL. Padrão do Spark: **200**. Neste projeto, 8 (laptop).
#
# **Por que importa.** Poucas partições → tasks grandes, pouca paralelização e risco de
# **spill** (§13). Partições demais → milhares de tasks minúsculas, em que o custo fixo de
# agendar cada task e buscar N×M blocos de shuffle domina. Ambos ficam lentos.
#
# **Como funciona.** Abaixo, a mesma agregação (`groupBy(repo_id)` sobre 2 milhões de linhas)
# com o AQE **desligado** (senão ele corrige o número sozinho — §3) e partições variando.

# %%
def agg_repo():
    return ev().groupBy("repo_id").agg(F.count("*").alias("n")).agg(F.max("n")).collect()


timings_parts = []
with spark_conf(spark, {"spark.sql.adaptive.enabled": "false"}):
    for p in (1, 8, 200, 2000):
        with spark_conf(spark, {"spark.sql.shuffle.partitions": str(p)}):
            timings_parts.append(time_it(agg_repo, repeat=3, warmup=1, label=f"shuffle.partitions={p}"))
print(compare(timings_parts))

# %% [markdown]
# O padrão é a "curva em U": 1 partição desperdiça 3 dos 4 núcleos; 2000 partições geram 2000
# tasks minúsculas — o custo fixo por task domina (é a linha que mais se afasta das outras na
# tabela). Os valores do meio (8 = 2× os núcleos, e 200, o padrão) ficam mais perto um do
# outro; qual dos dois ganha depende do volume. No cluster, a regra prática é mirar partições
# de shuffle de **~100–200 MB** cada e um múltiplo do total de núcleos.
#
# > 🎤 **Resposta de 30 s:** "`shuffle.partitions` define quantas tasks rodam depois de cada
# > shuffle. O padrão 200 é arbitrário: grande demais para dado pequeno, pequeno demais para
# > terabytes. Eu miro partições de 100–200 MB e múltiplos dos núcleos — e na prática deixo o
# > AQE coalescer a partir de um valor alto."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Vale para DataFrame/SQL. RDD usa `spark.default.parallelism`.
# - Com AQE, o número configurado vira um **teto inicial**: ele junta partições pequenas
#   (*coalesce*) até `advisoryPartitionSizeInBytes` (64 MB). Por isso a recomendação é
#   configurar alto e deixar o AQE reduzir.
# - No Databricks existe `spark.sql.shuffle.partitions=auto` (auto-optimized shuffle) ☁️.
# - Medir pela Spark UI: aba Stages, "Shuffle Read Size / Records" e a distribuição de duração
#   das tasks (min/mediana/max).
# </details>
#
# **Trade-offs:** um valor fixo serve a um tamanho de dado; job cujo volume varia muito (carga
# completa × incremental) precisa de AQE ou de configuração por etapa.

# %% [markdown]
# ## 3. AQE — Adaptive Query Execution
#
# **O que é.** Reotimização do plano **durante** a execução, usando estatísticas reais de cada
# stage terminado (tamanho de cada partição de shuffle), e não estimativas. Ligado por padrão
# desde o Spark 3.2. Faz três coisas:
#
# 1. **Coalesce de partições**: junta partições de shuffle pequenas;
# 2. **Troca de estratégia de join**: vira *broadcast* se um lado ficou pequeno de verdade;
# 3. **Skew join**: divide partições gigantes de um join (§5).
#
# **Por que importa.** O otimizador estático erra estimativas (filtro seletivo, UDF, dado sem
# estatística). O AQE corrige com números reais, sem ninguém mexer no código.
#
# **Nota de método — reuso de shuffle.** Rodar `.collect()` duas vezes no **mesmo** objeto
# DataFrame reaproveita os arquivos de shuffle da 1ª vez (stages aparecem como *skipped*) e a
# 2ª medição sai artificialmente rápida. Por isso `time_it` sempre recebe uma função que
# **monta a consulta de novo** (`ev()` devolve um DataFrame novo).

# %% [markdown]
# **3a. Coalesce.** Mesma agregação com `shuffle.partitions=2000`, AQE desligado × ligado.
# A tabela de stages mostra quantas tasks o stage pós-shuffle realmente rodou.

# %%
with spark_conf(spark, {"spark.sql.shuffle.partitions": "2000"}):
    with spark_conf(spark, {"spark.sql.adaptive.enabled": "false"}), job_group(spark, "s3-off"):
        t_off = time_it(agg_repo, repeat=3, warmup=1, label="2000 partições, AQE OFF")
    with job_group(spark, "s3-on"):
        t_on = time_it(agg_repo, repeat=3, warmup=1, label="2000 partições, AQE ON")
print(compare([t_off, t_on]), "\n")
for g in ("s3-off", "s3-on"):
    tasks = sorted({st.num_tasks for st in ui.stage_summaries(g)})
    print(f"{g}: nº de tasks por stage (distintos) = {tasks}")

# %% [markdown]
# Com AQE, o stage pós-shuffle deixa de ter 2000 tasks e passa a ter poucas (a lista de "nº de
# tasks por stage" acima; no plano final aparece `AQEShuffleRead coalesced`), e o tempo cai
# várias vezes — sem ninguém ter mexido no `shuffle.partitions`.
#
# **3b. Troca para broadcast em tempo de execução.** "Eventos de repositórios que publicaram
# release hoje": o lado direito é `ev().filter(type = 'ReleaseEvent').distinct()`. O otimizador
# estático não sabe quão seletivo é o filtro (estima pelo tamanho da tabela inteira) e planeja
# **sort-merge join**. Executado, o lado direito tem poucos KB → o AQE troca para **broadcast**.

# %%
releases = ev().filter("type = 'ReleaseEvent'").select("repo_id").distinct()
q_rel = ev().join(releases, "repo_id").groupBy("type").count()
print("ANTES de executar:", grep_plan(q_rel, "Join", "isFinalPlan"))
q_rel.collect()
print("DEPOIS de executar:", grep_plan(q_rel, "Join", "isFinalPlan"))

# %% [markdown]
# O plano final mostra `BroadcastHashJoin` e mantém o `SortMergeJoin` original como referência
# (o AQE registra os dois). Repare em `isFinalPlan=true`.
#
# > 🎤 **Resposta de 30 s:** "O AQE reotimiza o plano no meio da execução com o tamanho real de
# > cada shuffle: junta partições pequenas, troca sort-merge por broadcast quando um lado ficou
# > pequeno e quebra partições com skew em join. Ligado por padrão desde o 3.2 — o que eu faço é
# > deixar o `shuffle.partitions` alto e conferir no plano final (`isFinalPlan=true`)."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - O AQE só age em **fronteiras de shuffle** (é lá que ele tem estatística). Um scan sem
#   shuffle não é reotimizado.
# - `explain()` antes da action mostra `isFinalPlan=false`; o plano real só existe depois — na
#   Spark UI (aba SQL) ou chamando `explain` depois da action, como aqui.
# - Configurações: `spark.sql.adaptive.coalescePartitions.enabled`,
#   `advisoryPartitionSizeInBytes` (64 MB), `spark.sql.adaptive.autoBroadcastJoinThreshold`
#   (limite próprio do AQE para a troca em runtime), `skewJoin.*`.
# - **Dynamic Partition Pruning** (DPP, outra otimização de runtime): num join fato × dimensão
#   filtrada, o filtro da dimensão vira filtro de partição na fato.
# </details>
#
# **Trade-offs:** o AQE não conserta tudo — não melhora um scan de arquivos pequenos, não
# remove skew em agregação de janela (§5) e a troca para broadcast só acontece depois que o
# lado pequeno já foi para o shuffle (o broadcast estático evita esse shuffle).

# %% [markdown]
# ## 4. Joins: broadcast hash join × sort-merge join
#
# **O que é.**
# - **Sort-merge join (SMJ)**: os dois lados passam por shuffle pela chave, cada partição é
#   ordenada e as duas listas ordenadas são "costuradas". Escala para dois lados enormes.
# - **Broadcast hash join (BHJ)**: o lado pequeno é coletado no driver e enviado inteiro a cada
#   executor; cada task do lado grande faz lookup numa hash table local. **Sem shuffle do lado
#   grande.**
#
# **Por que importa.** Fato grande × dimensão pequena é o join mais comum de um lakehouse. Fazer
# shuffle de 2 bilhões de linhas para casar com 2 mil é desperdício.
#
# **Como funciona.** O Spark escolhe BHJ sozinho quando a estimativa de um lado é menor que
# `spark.sql.autoBroadcastJoinThreshold` (10 MB). Dá para forçar com o *hint* `broadcast()`
# ou impedir com o limite em `-1`. Aqui com AQE **desligado** para isolar a decisão estática.

# %%
cols = ["repo_id", "id", "type", "actor_login", "repo_name", "created_at"]


def join_bhj():
    return (ev().select(*cols).join(F.broadcast(top_repos()), "repo_id")
            .groupBy("type").agg(F.count("*"), F.max("id")).collect())


def join_smj():
    return (ev().select(*cols).join(top_repos(), "repo_id")
            .groupBy("type").agg(F.count("*"), F.max("id")).collect())


with spark_conf(spark, {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1"}):
    print("hint broadcast():", grep_plan(ev().join(F.broadcast(top_repos()), "repo_id"), "Join", "Exchange"))
    print("threshold = -1  :", grep_plan(ev().join(top_repos(), "repo_id"), "Join", "Exchange"), "\n")
    t_bhj = time_it(join_bhj, repeat=3, warmup=1, label="broadcast hash join")
    t_smj = time_it(join_smj, repeat=3, warmup=1, label="sort-merge join")
print(compare([t_bhj, t_smj]))
with spark_conf(spark, {"spark.sql.adaptive.enabled": "false"}):
    print("\nsem hint, threshold padrão (10 MB):", grep_plan(ev().join(top_repos(), "repo_id"), "Join"))

# %% [markdown]
# No plano do SMJ aparecem dois `Exchange hashpartitioning(repo_id)` e dois `Sort`; no BHJ, um
# `BroadcastExchange` só do lado pequeno. Sem hint, o Spark já escolheu BHJ porque a `top_repos`
# tem poucos KB. **Sobre o tempo:** local, a diferença entre os dois é pequena — o "shuffle" do
# SMJ é disco local na mesma máquina, sobre 2 milhões de linhas — e, com a máquina sob carga,
# as duas medianas ficam dentro do ruído (podem até sair invertidas na tabela acima). A prova
# aqui é o **plano**: o BHJ não tem `Exchange` do lado grande. No cluster, esse `Exchange` é o
# lado grande inteiro serializado, gravado e trafegado pela rede — a diferença cresce com o volume.
#
# > 🎤 **Resposta de 30 s:** "Broadcast join manda a tabela pequena inteira para cada executor
# > e evita o shuffle do lado grande; sort-merge faz shuffle e sort dos dois lados e escala para
# > tabelas grandes. O Spark faz broadcast sozinho abaixo de 10 MB; acima disso eu uso hint
# > quando sei que a dimensão cabe na memória dos executores — e confiro no plano."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Limites do broadcast:** a tabela passa pelo driver (coleta) e fica em memória em todo
#   executor. Broadcast de 2 GB = OOM no driver ou executores lentos de GC. Teto rígido de 8 GB.
# - **Shuffle hash join**: shuffle nos dois lados, mas sem sort (hash table por partição) —
#   útil quando um lado é médio. Hint `shuffle_hash`.
# - **Hints SQL**: `/*+ BROADCAST(d) */`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`.
# - Estimativa de tamanho vem de estatísticas: do Delta (tamanho dos arquivos) ou de
#   `ANALYZE TABLE ... COMPUTE STATISTICS`. Sem estatística, filtros seletivos enganam (§3b).
# - Broadcast não funciona para o lado "preservado" de outer join (ex.: em `left join`, só o
#   lado direito pode ser broadcast).
# </details>
#
# **Trade-offs:** hint de broadcast fixo no código envelhece mal — a "dimensão pequena" cresce
# e um dia derruba o job. Prefira deixar o limite automático + AQE e documentar hints.

# %% [markdown]
# ## 5. Skew: quando uma chave tem muito mais linhas que as outras
#
# **O que é.** *Data skew* (assimetria de dados): a distribuição por chave é desigual, e todas
# as linhas de uma chave caem na **mesma** partição de shuffle. Uma task fica com uma fração
# enorme do trabalho — a *straggler* — e o stage inteiro espera por ela.
#
# **Por que importa.** É a causa nº 1 de "o job travou em 199/200 tasks". No GH Archive, o
# `github-actions[bot]` responde por ~13% dos eventos deste dia (tabela abaixo). Com 200
# partições, a média por partição é 0,5% — a do bot tem ~25 vezes isso.
#
# **Como funciona (o diagnóstico).** Join `events × actors` por `actor_login`, AQE e broadcast
# desligados, 200 partições. Pela REST API da Spark UI (a mesma da aba Stages) lemos, do stage
# do join, a distribuição de **registros lidos por task** (determinística) e de **duração**.

# %%
top_actors = (ev().groupBy("actor_login").count().orderBy(F.desc("count")).limit(3)
              .withColumn("pct", F.round(100 * F.col("count") / N_EVENTS, 1)))
top_actors.show(truncate=False)


def skew_query():
    # id e repo_name (alta cardinalidade) deixam o tamanho em BYTES proporcional às linhas — ver nota.
    return (ev().select("actor_login", "type", "id", "repo_name").join(actors(), "actor_login")
            .groupBy("type").agg(F.count("*"), F.sum("n_events"), F.max("id")))


def skew_report(group: str) -> dict:
    st = ui.heaviest_shuffle_stage(group)
    rec = ui.task_shuffle_records(st["stageId"], st["attemptId"])
    dur = ui.task_durations(st["stageId"], st["attemptId"])
    print(f"  registros/task: {rec}")
    print(f"  duração (ms)  : {dur}")
    return {"tasks": rec.n, "rec_max/med": round(rec.max_over_median, 1), "dur_max/med": round(dur.max_over_median, 1)}


NO_AQE = {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1",
          "spark.sql.shuffle.partitions": "200"}
skew_rows = []
with spark_conf(spark, NO_AQE), job_group(spark, "s5-skew"):
    t_skew = time_it(lambda: skew_query().collect(), repeat=3, warmup=1, label="join com skew (sem AQE)")
print(t_skew)
skew_rows.append({"variante": "sem tratamento", "mediana_s": round(t_skew.median, 2), **skew_report("s5-skew")})

# %% [markdown]
# A task mais pesada leu dezenas de vezes mais registros que a típica — é a partição do bot.
# Em duração, a razão max/mediana é menor que a de registros porque, local, cada task tem um
# custo fixo alto (abrir blocos, codegen) que "achata" a diferença. Em produção, com o bot
# tendo mais de 10% de **bilhões** de linhas, a straggler leva horas enquanto as outras levam minutos.
#
# **Conserto 1 — salting seletivo.** Acrescenta à chave um "sal" aleatório 0..N-1 **só nas
# chaves quentes**; o lado pequeno replica essas chaves N vezes. O join vira por
# `(actor_login, sal)` e o bot se espalha em N partições. Implementado em
# `oss_lakehouse.perf.salted_join` (com teste que prova resultado idêntico ao join comum).

# %%
hot_keys = [r.actor_login for r in ev().groupBy("actor_login").count()
            .filter(F.col("count") > N_EVENTS * 0.01).collect()]
print("chaves quentes (>1% dos eventos):", hot_keys)


def salted_query():
    big = ev().select("actor_login", "type", "id", "repo_name")
    return (salted_join(big, actors(), "actor_login", hot_keys, buckets=32)
            .groupBy("type").agg(F.count("*"), F.sum("n_events"), F.max("id")))


assert sorted(salted_query().collect()) == sorted(skew_query().collect()), "salting mudou o resultado!"
with spark_conf(spark, NO_AQE), job_group(spark, "s5-salt"):
    t_salt = time_it(lambda: salted_query().collect(), repeat=3, warmup=1, label="join com salting seletivo")
print(t_salt)
skew_rows.append({"variante": "salting seletivo (32)", "mediana_s": round(t_salt.median, 2),
                  **skew_report("s5-salt")})

# %% [markdown]
# **Conserto 2 — deixar o AQE resolver.** Com `spark.sql.adaptive.skewJoin.enabled` (padrão
# ligado), uma partição é considerada skew se for > `skewedPartitionFactor` (5) × a mediana
# **e** > `skewedPartitionThresholdInBytes` (256 MB). Ela é dividida em pedaços e o lado
# pequeno correspondente é replicado — o salting, feito pelo motor. Nossos dados são pequenos
# demais para 256 MB, então baixamos o limiar para 2 MB (o mecanismo é o mesmo).
#
# **Nota de sênior — o AQE mede skew em bytes, não em linhas.** No primeiro protótipo deste
# notebook o join levava só `actor_login` e `type`: a partição do bot tinha 25× as linhas da
# mediana mas só ~3× os **bytes**, porque o mesmo texto `github-actions[bot]` repetido comprime
# muito bem no shuffle. O AQE não a considerou skew. Com colunas de alta cardinalidade (`id`,
# `repo_name`), os bytes acompanham as linhas e o AQE age.

# %%
AQE_SKEW = {"spark.sql.adaptive.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "2m",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "2m"}
with spark_conf(spark, AQE_SKEW):
    with job_group(spark, "s5-aqe"):
        t_aqe = time_it(lambda: skew_query().collect(), repeat=3, warmup=1, label="join com AQE skew join")
    q = skew_query()
    q.collect()
    print(grep_plan(q, "AQEShuffleRead", "SortMergeJoin")[:4])
print(t_aqe)
skew_rows.append({"variante": "AQE skew join", "mediana_s": round(t_aqe.median, 2), **skew_report("s5-aqe")})
print("\n" + as_table(skew_rows))

# %% [markdown]
# `AQEShuffleRead coalesced and skewed`: o AQE juntou as partições pequenas **e** dividiu a do
# bot. A razão max/mediana de registros e de duração cai para perto de 2–3×.
#
# **Como ler a coluna `mediana_s` (tempo total).** Os dois consertos reduzem o **skew** — as
# colunas `rec_max/med` e `dur_max/med`, que são o que este experimento prova. **Não espere
# ganho no tempo total** nesta escala: a straggler que eles eliminam dura menos de 1 s (veja o
# `max` de duração), então não há o que economizar, e o salting ainda cobra o seu preço
# (`rand()`, o `explode` do lado pequeno, uma chave de join a mais) — ele tende a sair **mais
# lento** que o join sem tratamento. O AQE, além de dividir a partição do bot, junta as 200
# partições em poucas dezenas (coluna `tasks`). Salting compensa quando a straggler custa
# minutos ou horas (ou estoura a memória); em dado pequeno é custo sem retorno. É o tipo de
# resultado local que não se deve "arredondar" a favor da teoria.
#
# **E na agregação e na janela?** Três casos, medidos abaixo em registros lidos por task:
#
# 1. `groupBy(actor_login).count()` quase não sofre skew: a **agregação parcial** (map-side
#    combine) reduz as ~260 mil linhas do bot a uma linha por task de leitura antes do shuffle.
# 2. Janela **top-N** (`row_number() ... rn = 1`): desde o Spark 3.5 o otimizador insere um
#    `WindowGroupLimit` **parcial** antes do shuffle — cada task já descarta o que não pode ser
#    o 1º da chave. O skew também some (e a distribuição fica igual à da agregação).
# 3. Janela **que precisa de todas as linhas** (`lag`, `lead`, soma acumulada): não há redução
#    possível antes do shuffle; todas as linhas do bot vão para uma task. AQE skew join **não**
#    atua aqui (não é join).

# %%
with spark_conf(spark, {"spark.sql.adaptive.enabled": "false", "spark.sql.shuffle.partitions": "200"}):
    with job_group(spark, "s5-agg"):
        ev().groupBy("actor_login").count().agg(F.max("count")).collect()
    w = Window.partitionBy("actor_login").orderBy("created_at")
    base_w = lambda: ev().select("actor_login", "created_at", "id")  # noqa: E731
    q_topn = base_w().withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    with job_group(spark, "s5-topn"):
        q_topn.count()
    with job_group(spark, "s5-lag"):  # intervalo desde o evento anterior do mesmo ator
        (base_w().withColumn("prev", F.lag("created_at").over(w))
         .agg(F.max(F.col("created_at").cast("long") - F.col("prev").cast("long"))).collect())
    print("plano da janela top-N:", grep_plan(q_topn, "WindowGroupLimit"), "\n")
for g, name in (("s5-agg", "groupBy+count"), ("s5-topn", "janela top-N (rn = 1)"), ("s5-lag", "janela lag()")):
    st = ui.heaviest_shuffle_stage(g)
    print(f"{name:<22} registros/task: {ui.task_shuffle_records(st['stageId'], st['attemptId'])}")

# %% [markdown]
# A janela com `lag()` repete o padrão do join sem tratamento (max dezenas de vezes a mediana);
# a agregação e a janela top-N, não. Conserto para o caso 3: salting não serve direto (a janela
# precisa das linhas da chave juntas e em ordem) — as saídas são tratar a chave quente à parte,
# reduzir o dado antes da janela (filtrar, pré-agregar) ou dar mais memória/partições e aceitar.

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Skew é quando uma chave concentra muitas linhas e uma task vira a
# > straggler. Diagnostico na Spark UI: max muito acima da mediana em duração e shuffle read do
# > stage. Primeiro deixo o AQE skew join agir; se não basta, faço salting só nas chaves quentes;
# > em janela, onde nem AQE nem salting atuam, trato a chave quente à parte (filtrar o bot,
# > processar separado, unir). E sempre pergunto: essa chave deveria estar aqui? `null` como chave
# > de join é um skew clássico."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Chave nula**: em `left join`, todas as linhas com chave `null` vão para a mesma partição
#   e nunca casam. Filtrar ou substituir por valor aleatório antes do join.
# - **Salting tem custo**: replica o lado pequeno N vezes. Salgar todas as chaves com N=32
#   multiplicaria as ~450 mil linhas de `actors` por 32; o seletivo só replica as chaves quentes.
# - **Broadcast mata o skew** de join: sem shuffle, não há partição quente. Se a dimensão cabe,
#   é a melhor saída.
# - **Skew em escrita**: `partitionBy` numa coluna desbalanceada gera um arquivo gigante.
# - **Duas fases para agregação não-associativa**: agregar por `(chave, sal)` e depois por `chave`.
# - Sinais na UI: "Summary Metrics" do stage (Max ≫ 75th percentile), task com spill, GC alto.
# </details>
#
# **Trade-offs:** salting complica o código e precisa saber quais são as chaves quentes (que
# mudam com o tempo). AQE é grátis, mas só cobre join com shuffle e depende dos limiares.

# %% [markdown]
# ## 6. Small files: o mesmo dado em 2000 arquivos × compactado
#
# **O que é.** Uma tabela com milhares de arquivos de poucos KB/MB. Nasce de streaming com
# micro-lotes frequentes, de `repartition` exagerado, de `partitionBy` em coluna de alta
# cardinalidade ou de ingestão "arquivo por evento".
#
# **Por que importa.** Cada arquivo custa uma listagem, uma abertura, a leitura do rodapé
# parquet e uma entrada no `_delta_log`. No object storage (ADLS, S3) cada abertura é uma
# requisição HTTP com dezenas de ms de latência. O tempo vai para *overhead*, não para dado.
#
# **Como funciona.** Mesmo conteúdo (2 milhões de linhas, sem `payload`) em 2000 arquivos e em
# 4. Depois `OPTIMIZE`, o *bin-packing* do Delta: reescreve arquivos pequenos em poucos grandes
# num commit novo (os antigos viram `remove` no log — continuam no disco até o `VACUUM`).

# %%
SMALL, COMPACT = f"{D}/small_files", f"{D}/compact"
base_cols = ["id", "type", "actor_login", "repo_id", "created_at", "event_hour"]
build_once(SMALL, lambda p: ev().select(*base_cols).repartition(2000)
           .write.format("delta").mode("overwrite").save(p))
build_once(COMPACT, lambda p: ev().select(*base_cols).coalesce(4)
           .write.format("delta").mode("overwrite").save(p))
# Execuções anteriores deste notebook já compactaram a SMALL: volta ao estado "bagunçado".
if spark.sql(f"DESCRIBE HISTORY delta.`{SMALL}`").agg(F.max("version")).first()[0] > 0:
    spark.sql(f"RESTORE TABLE delta.`{SMALL}` TO VERSION AS OF 0")
print("small_files:", detail(SMALL), "| compact:", detail(COMPACT))


def read_agg(path):
    return lambda: spark.read.format("delta").load(path).groupBy("type").count().collect()


t_small = time_it(read_agg(SMALL), repeat=3, warmup=1, label="leitura: 2000 arquivos")
t_comp = time_it(read_agg(COMPACT), repeat=3, warmup=1, label="leitura: 4 arquivos")
with stopwatch() as sw_opt:
    m = spark.sql(f"OPTIMIZE delta.`{SMALL}`").select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
print(f"OPTIMIZE em {sw_opt.seconds:.1f}s: {m.numFilesRemoved} arquivos removidos → {m.numFilesAdded} adicionado(s)")
t_after = time_it(read_agg(SMALL), repeat=3, warmup=1, label="leitura: depois do OPTIMIZE")
print(compare([t_small, t_comp, t_after]))

# %% [markdown]
# Mesmo dado, mesma consulta: a versão com 2000 arquivos é a mais lenta por uma margem larga
# (coluna da direita) e ocupa quase o dobro em disco (arquivo pequeno comprime pior e repete
# rodapé). Um `OPTIMIZE` devolve o tempo para perto do da tabela compactada. Local, abrir arquivo é
# barato (disco do laptop); no ADLS, onde cada abertura é uma requisição HTTP, a diferença é
# maior.
#
# **Quanto custa gerar small files?** Gravar 1 hora de eventos em 300 arquivos × 1 arquivo:

# %%
one_hour = lambda: ev().filter("event_hour = 12").select(*base_cols)  # noqa: E731
t_w_small = time_it(lambda: one_hour().repartition(300).write.format("delta").mode("overwrite")
                    .save(f"{D}/write_small"), repeat=3, label="escrita: 1 hora em 300 arquivos")
t_w_one = time_it(lambda: one_hour().coalesce(1).write.format("delta").mode("overwrite")
                  .save(f"{D}/write_one"), repeat=3, label="escrita: 1 hora em 1 arquivo")
print(compare([t_w_small, t_w_one]))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Small files matam a leitura: o custo vira abrir arquivo e ler
# > metadado, não processar dado — e no ADLS cada abertura é uma requisição. Resolvo na origem
# > (micro-lotes maiores, optimized writes, não particionar demais) e compacto com `OPTIMIZE`;
# > no Databricks, auto compaction e Predictive Optimization fazem isso sozinhos. Alvo: arquivos
# > de 100 MB a 1 GB."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Optimized writes** (`delta.autoOptimize.optimizeWrite`): um shuffle antes de gravar para
#   cada partição sair com arquivos grandes. **Auto compaction** (`delta.autoOptimize.autoCompact`):
#   um OPTIMIZE leve, síncrono, logo depois da escrita. No OSS existem como
#   `spark.databricks.delta.optimizeWrite.enabled` / `autoCompact.enabled`.
# - `OPTIMIZE` é **idempotente** e não bloqueia leitores (snapshot isolation). Pode conflitar
#   com `UPDATE/DELETE` concorrente nos mesmos arquivos.
# - Os arquivos antigos continuam no storage (time travel) até o `VACUUM` (notebook 10).
# - `OPTIMIZE ... WHERE event_date >= ...` compacta só as partições recentes.
# - No streaming: `trigger(availableNow=True)` ou intervalos maiores reduzem arquivos por lote.
# </details>
#
# **Trade-offs:** OPTIMIZE reescreve dado (custo de compute e storage); arquivo grande demais
# piora o data skipping (§7) e o paralelismo de leitura.

# %% [markdown]
# ## 7. Data skipping: estatísticas min/max, Z-order e Liquid Clustering
#
# **O que é.** O Delta guarda no `_delta_log`, para cada arquivo, **min, max e contagem de
# nulos** das primeiras 32 colunas (`delta.dataSkippingNumIndexedCols`). Um filtro
# `repo_id = X` pula todo arquivo cujo intervalo `[min, max]` não contém X — *data skipping*.
#
# **Por que importa.** O skipping só funciona se o dado estiver **agrupado** pela coluna do
# filtro. Se cada arquivo tem um pouco de cada `repo_id`, todo intervalo cobre o domínio
# inteiro e nada é pulado. **Z-order** e **Liquid Clustering** reorganizam o layout para que
# valores próximos fiquem no mesmo arquivo.
#
# **Como funciona.** 7a: o skipping "de graça" pela ordem de chegada (`created_at`). 7b: tabela
# com linhas embaralhadas em 32 arquivos, consulta pontual por `repo_id` antes e depois do
# `OPTIMIZE ZORDER BY`. 7c: o mesmo com Liquid Clustering (`CLUSTER BY`) — suportado no Delta
# OSS 4.x, roda local. Medimos **arquivos lidos** pela métrica `number of files read` do scan.

# %%
with job_group(spark, "s7-time"):
    n13 = ev().filter("created_at >= '2026-10-01 13:00:00' AND created_at < '2026-10-01 13:10:00'").count()
with job_group(spark, "s7-actor"):
    n_bot = ev().filter("actor_login = 'dependabot[bot]'").count()
print(f"filtro por 10 minutos de created_at: {n13:,} linhas, arquivos lidos = {files_read(ui, 's7-time')} de 24")
print(f"filtro por actor_login            : {n_bot:,} linhas, arquivos lidos = {files_read(ui, 's7-actor')} de 24")
stats = delta_active_files(EVENTS)
print(as_table([{"arquivo": f["path"][:18], "linhas": f["num_records"],
                 "min created_at": f["min"]["created_at"][:19], "max created_at": f["max"]["created_at"][:19]}
                for f in stats[:4]]))

# %% [markdown]
# Cada arquivo cobre ~1 hora (cada `.json.gz` horário virou um arquivo), então um filtro de
# tempo de 10 minutos lê 1 arquivo de 24. Já `actor_login` aparece em todo arquivo — nenhum skipping.
#
# **7b. Z-order.** `OPTIMIZE ... ZORDER BY (repo_id)` reescreve a tabela ordenando por uma
# curva de Z (intercala os bits das colunas; com 1 coluna equivale a ordenar). Limitamos o
# arquivo de saída a 16 MB (`spark.databricks.delta.optimize.maxFileSize`; padrão 1 GB) para
# sobrarem vários arquivos nesta tabela pequena.

# %%
ZT = f"{D}/zorder"
ev().select("id", "type", "actor_login", "repo_id", "repo_name", "created_at") \
    .repartition(32).write.format("delta").mode("overwrite").save(ZT)
probe_repo = top_repos().orderBy("n", "repo_id").first()["repo_id"]


def files_for(path: str, group: str) -> tuple[int, int]:
    with job_group(spark, group):
        n = spark.read.format("delta").load(path).filter(F.col("repo_id") == probe_repo).count()
    return n, files_read(ui, group)


def ranges(path: str, k: int = 4) -> str:
    fs = delta_active_files(path)
    return as_table([{"arquivo": f["path"][:14], "linhas": f["num_records"],
                      "min repo_id": f["min"]["repo_id"], "max repo_id": f["max"]["repo_id"]} for f in fs[:k]])


n0, f0 = files_for(ZT, "s7-z0")
print(f"ANTES: {detail(ZT)} | repo_id={probe_repo}: {n0} linhas, arquivos lidos = {f0}")
print(ranges(ZT))
with spark_conf(spark, {"spark.databricks.delta.optimize.maxFileSize": str(16 * 1024 * 1024)}), stopwatch() as sw_z:
    zm = spark.sql(f"OPTIMIZE delta.`{ZT}` ZORDER BY (repo_id)").select("metrics.numFilesAdded",
                                                                        "metrics.numFilesRemoved").first()
n1, f1 = files_for(ZT, "s7-z1")
print(f"\nZORDER em {sw_z.seconds:.0f}s ({zm.numFilesRemoved} → {zm.numFilesAdded} arquivos)")
print(f"DEPOIS: {detail(ZT)} | repo_id={probe_repo}: {n1} linhas, arquivos lidos = {f1}")
print(ranges(ZT))

# %% [markdown]
# Antes: todo arquivo vai de um `repo_id` mínimo a um máximo quase iguais aos da tabela → lê
# tudo. Depois: intervalos **disjuntos** → a consulta pontual lê 1 arquivo.
#
# **7c. Liquid Clustering (local, Delta OSS).** `CLUSTER BY (repo_id)` na criação; o
# `OPTIMIZE` (sem `ZORDER`) aplica o clustering de forma **incremental** (só reorganiza o que
# ainda não está clusterizado). As chaves podem mudar com `ALTER TABLE ... CLUSTER BY` sem
# reescrever a tabela, e substitui partição + Z-order.

# %%
LC = f"{D}/liquid"
shutil.rmtree(LC, ignore_errors=True)
spark.sql(f"""CREATE TABLE delta.`{LC}` (id STRING, type STRING, actor_login STRING, repo_id BIGINT,
              repo_name STRING, created_at TIMESTAMP) USING delta CLUSTER BY (repo_id)""")
ev().select("id", "type", "actor_login", "repo_id", "repo_name", "created_at").repartition(32) \
    .write.format("delta").mode("append").save(LC)
n2, f2 = files_for(LC, "s7-lc0")
print(f"liquid antes do OPTIMIZE: {detail(LC)} | arquivos lidos = {f2}")
with spark_conf(spark, {"spark.databricks.delta.optimize.maxFileSize": str(16 * 1024 * 1024)}), stopwatch() as sw_l:
    spark.sql(f"OPTIMIZE delta.`{LC}`").collect()
n3, f3 = files_for(LC, "s7-lc1")
print(f"liquid depois do OPTIMIZE ({sw_l.seconds:.0f}s): {detail(LC)} | {n3} linhas, arquivos lidos = {f3}")
print(spark.sql(f"DESCRIBE DETAIL delta.`{LC}`").select("clusteringColumns", "tableFeatures").first())

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "O Delta guarda min/max por arquivo e pula arquivos que não podem
# > ter o valor filtrado. Isso só funciona se o dado estiver agrupado pela coluna do filtro —
# > Z-order e Liquid Clustering fazem esse agrupamento. Hoje, em tabela nova, uso Liquid: é
# > incremental, aceita trocar as chaves sem reescrever e substitui partição + Z-order. Escolho
# > as colunas pelos filtros mais frequentes, de alta cardinalidade."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Z-order em várias colunas** dilui o efeito de cada uma (a curva intercala); mais de 3–4
#   colunas raramente compensa. E o Z-order não é incremental: reescreve tudo o que otimiza.
# - Liquid usa curva de **Hilbert** (melhor localidade que Z) e guarda o estado de clustering
#   no log (*clustering domain metadata* — veja `domainMetadata` em `tableFeatures`).
# - Estatísticas só existem para as colunas indexadas (32 primeiras, ou
#   `delta.dataSkippingStatsColumns`): coloque as colunas de filtro no início ou configure.
# - Filtro com função na coluna (`lower(actor_login) = ...`) **não** usa skipping.
# - Coluna de baixa cardinalidade (ex.: `type`, 16 valores) é melhor como partição que como
#   clustering — e muitas vezes nem precisa.
# </details>
#
# **Trade-offs:** clusterizar custa reescrita (compute); escolher a coluna errada é pagar sem
# benefício. Liquid exige `writer` com suporte à feature (`clustering`) — leitores antigos ok.

# %% [markdown]
# ## 8. Particionamento: quando ajuda e o over-partitioning
#
# **O que é.** `partitionBy(col)` grava uma pasta por valor (`event_hour=13/`). Filtro na coluna
# de partição pula pastas inteiras sem nem olhar estatísticas (*partition pruning*).
#
# **Por que importa.** Ajuda quando a coluna é de **baixa cardinalidade**, aparece em quase
# todo filtro e cada partição tem **≥ 1 GB**. Particionar por coluna de cardinalidade alta, ou
# por duas colunas cujo produto explode, gera milhares de partições de poucos KB:
# **over-partitioning** = small files por construção.
#
# **Como funciona.** O mesmo dado (sem `payload`) sem partição, por `event_hour` (24) e por
# `(event_hour, type)` (~380). Medimos consulta de 1 hora e consulta da tabela inteira.

# %%
part_layouts = {"sem partição": [], "por event_hour": ["event_hour"], "por event_hour,type": ["event_hour", "type"]}
part_rows, part_t = [], []
for name, cols_p in part_layouts.items():
    path = f"{D}/part_" + ("_".join(cols_p) or "none")
    build_once(path, lambda p, c=cols_p: ev().drop("payload").write.format("delta")
               .partitionBy(*c).mode("overwrite").save(p))
    with job_group(spark, f"s8-{name}"):
        spark.read.format("delta").load(path).filter("event_hour = 13").groupBy("type").count().collect()
    t1 = time_it(lambda p=path: spark.read.format("delta").load(p).filter("event_hour = 13")
                 .groupBy("type").count().collect(), repeat=3, warmup=1, label=f"{name}: 1 hora")
    tf = time_it(lambda p=path: spark.read.format("delta").load(p).groupBy("type").count().collect(),
                 repeat=3, warmup=1, label=f"{name}: tabela toda")
    part_t += [t1, tf]
    part_rows.append({"layout": name, **detail(path), "arquivos lidos (1 h)": files_read(ui, f"s8-{name}"),
                      "1 hora (s)": round(t1.median, 2), "tabela toda (s)": round(tf.median, 2)})
print(as_table(part_rows))
print("\nplano (1 hora, por event_hour):",
      grep_plan(spark.read.format("delta").load(f"{D}/part_event_hour").filter("event_hour = 13"),
                "PartitionFilters", mode="formatted")[:1])

# %% [markdown]
# Leitura, primeiro pelo que é determinístico (arquivos): a partição por hora lê só o arquivo
# da hora 13 (`PartitionFilters` no plano); a tabela sem partição **também** pula arquivos,
# graças ao data skipping de §7 (os arquivos vieram mais ou menos agrupados por hora). O
# over-partitioning por `(event_hour, type)` transforma os mesmos ~90 MB em centenas de
# arquivos de poucas centenas de KB, e a consulta de 1 hora passa a abrir **mais** arquivos que
# a partição simples — pagou-se a explosão de arquivos sem ganhar nada.
#
# Agora os tempos: nesta escala os três layouts ficam **na mesma ordem de grandeza**, com
# diferenças do tamanho do ruído da máquina — não dá para afirmar ganho de tempo de nenhum
# deles. É o resultado honesto de uma tabela de ~90 MB em disco local: partição só paga quando
# cada partição tem volume (≥ 1 GB) e o custo por arquivo é o do object storage. O que este experimento prova é o **mecanismo**
# (pruning por pasta) e o **custo** (número de arquivos), não um ganho de tempo.
#
# > 🎤 **Resposta de 30 s:** "Particiono só por coluna de baixa cardinalidade que está em quase
# > todo filtro — normalmente data — e só se cada partição tiver pelo menos ~1 GB. Abaixo disso,
# > estatísticas + clustering fazem o mesmo trabalho sem small files. A recomendação atual do
# > Databricks é não particionar tabela menor que ~1 TB e usar Liquid Clustering."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Partição não muda depois** sem reescrever a tabela; Liquid muda as chaves com `ALTER`.
# - Partição ajuda em **isolamento de concorrência** (notebook 10: `ConcurrentAppendException`
#   se evita com condição de partição) e em `replaceWhere` / sobrescrita de uma partição.
# - Partição por data + Z-order/Liquid dentro dela era o padrão antigo; Liquid sozinho cobre os dois.
# - Particionar pela coluna errada (ex.: `type`, quando o filtro é por data) não ajuda nenhuma
#   consulta e ainda multiplica arquivos.
# </details>
#
# **Trade-offs:** partição é a forma mais barata de pruning **quando acertada** e a mais cara de
# desfazer quando errada.

# %% [markdown]
# ## 9. Predicate pushdown e column pruning
#
# **O que é.** *Predicate pushdown*: o filtro desce até a leitura e o leitor parquet usa as
# estatísticas de cada *row group* para nem decodificar o que não passa. *Column pruning*: só as
# colunas usadas são lidas — parquet é colunar, cada coluna fica num bloco separado.
#
# **Por que importa.** A coluna `payload` é a maior parte dos bytes da tabela. Uma consulta que
# não a usa e mesmo assim faz `select *` paga a leitura e a descompressão dela inteira.

# %%
q_push = ev().filter("type = 'ReleaseEvent' AND repo_id > 1000").select("repo_id", "actor_login")
print("\n".join(grep_plan(q_push, "PushedFilters", "ReadSchema", "DataFilters", mode="formatted")))
t_2 = time_it(lambda: ev().agg(F.count("actor_login"), F.max("type")).collect(), 3, "2 colunas", warmup=1)
t_p = time_it(lambda: ev().agg(F.count("actor_login"), F.max("type"), F.max("payload")).collect(), 3,
              "2 colunas + payload", warmup=1)
print(compare([t_2, t_p]))
slug_udf = F.udf(lambda s: s.lower() if s else None, StringType())
print("\nfiltro com UDF Python:", grep_plan(ev().filter(slug_udf("type") == "releaseevent").select("repo_id"),
                                              "PushedFilters", "BatchEvalPython", "Filter"))

# %% [markdown]
# `ReadSchema` lista só `type, actor_login, repo_id`; `PushedFilters` traz as duas condições.
# Ler o `payload` multiplica o tempo (coluna da direita). E o filtro via **UDF Python** não é
# empurrado: o scan sai com `PushedFilters: []` (vazio) — a tabela inteira é lida, a UDF roda
# no `BatchEvalPython` e só **depois** vem o `Filter`. O leitor não sabe executar Python.
#
# > 🎤 **Resposta de 30 s:** "Pushdown leva o filtro para o leitor, que pula row groups e
# > arquivos pelas estatísticas; pruning lê só as colunas usadas. Os dois aparecem no plano como
# > `PushedFilters` e `ReadSchema`. O que quebra: `select *` desnecessário, filtro com UDF, cast
# > na coluna e filtro aplicado depois de um `cache`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Quatro níveis de pulo**: partição (pasta) → arquivo (stats do Delta) → row group (stats do
#   rodapé parquet) → página (column index).
# - JSON/CSV não têm pruning real: o arquivo inteiro é lido e parseado — por isso bronze em Delta.
# - Pushdown através de `join`: o Catalyst empurra filtros para os dois lados quando é seguro.
# - `PushedFilters` lista o que foi oferecido ao leitor; parquet ainda reaplica o filtro por linha.
# </details>
#
# **Trade-offs:** nenhum — é grátis. O trabalho é não sabotar (UDF em filtro, `select *`).

# %% [markdown]
# ## 10. Cache e persist: quando ajuda e quando atrapalha
#
# **O que é.** `df.cache()` (= `persist(MEMORY_AND_DISK)`) guarda o resultado de um DataFrame na
# memória dos executores (formato colunar comprimido) na **primeira action**; as seguintes leem
# do cache em vez de recalcular.
#
# **Por que importa.** Ajuda quando um resultado **caro** é reusado **várias vezes** na mesma
# sessão (ex.: exploração, ML iterativo, várias saídas a partir do mesmo intermediário).
# Atrapalha quando é usado uma vez só, quando a fonte já é rápida (Delta com skipping) ou quando
# ocupa a memória que o shuffle precisaria (→ spill, §13).

# %%
base_c = lambda: ev().select("type", "actor_login", "repo_id").filter("type != 'PushEvent'")  # noqa: E731


def heavy(df):
    return df.groupBy("type").agg(F.countDistinct("actor_login"), F.countDistinct("repo_id")).collect()


t_nc = time_it(lambda: heavy(base_c()), repeat=3, warmup=1, label="sem cache (recalcula do Delta)")
cached = base_c().cache()
with stopwatch() as sw_mat:
    n_cached = cached.count()  # 1ª action: materializa
t_c = time_it(lambda: heavy(cached), repeat=3, warmup=1, label="com cache")
print(compare([t_nc, t_c]))
print(f"materializar o cache: {sw_mat.seconds:.2f}s ({n_cached:,} linhas)")
mem = [r for r in ui.get("storage/rdd") if "Delta Table State" not in r["name"]]
print("aba Storage:", [(r["name"][:40], f"{r['memoryUsed'] / 1e6:.1f} MB em memória") for r in mem])
print("plano lendo do cache:", grep_plan(cached.groupBy("type").count(), "InMemoryTableScan")[:1])
cached.unpersist()

# %% [markdown]
# Com cache, cada reuso fica mais barato — mas a 1ª action paga a materialização. Só compensa
# se **soma(reusos economizados) > custo de materializar + memória ocupada**.
#
# > 🎤 **Resposta de 30 s:** "Cache vale para resultado caro reusado várias vezes na mesma
# > sessão. Não uso por reflexo: em pipeline de uma passada ele só gasta memória, pode causar
# > spill e esconde mudanças na fonte. Sempre `unpersist` no fim e confiro na aba Storage."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Níveis: `MEMORY_ONLY`, `MEMORY_AND_DISK` (padrão do DataFrame), `DISK_ONLY`, `_SER`, `_2`
#   (replicado). `cache()` é preguiçoso; `count()` materializa.
# - **Cache não é checkpoint**: guarda o dado mas mantém a linhagem. `checkpoint()` corta a
#   linhagem (útil em plano gigante iterativo).
# - **Databricks disk cache** (antigo Delta cache) ☁️: cache automático dos arquivos parquet
#   no SSD local dos workers — transparente, sem `cache()`. Em serverless/SQL warehouse é o padrão.
# - Cache de tabela Delta fica **velho** se a tabela mudar — o resultado não acompanha o commit.
# </details>
#
# **Trade-offs:** memória é o recurso mais disputado do executor; cache compete com execução.

# %% [markdown]
# ## 11. UDF Python × função nativa × pandas UDF (Arrow)
#
# **O que é.**
# - **Função nativa** (`F.lower`, `F.regexp_replace`): roda na JVM, entra no codegen.
# - **UDF Python** (`F.udf`): cada linha é serializada da JVM para um processo Python, processada
#   e devolvida — linha a linha (pickle). Quebra codegen e pushdown.
# - **pandas UDF** (`F.pandas_udf`): o mesmo, mas em **lotes colunares Apache Arrow** —
#   vetorizado com pandas/NumPy, sem pickle por linha.
#
# **Por que importa.** UDF Python é o motivo mais comum de "o PySpark é lento". Quase sempre
# existe função nativa equivalente.

# %%
def slug_py(s):
    return s.lower().replace("[bot]", "") if s else None


# `useArrow=False` fixa o caminho CLÁSSICO (pickle, linha a linha). Sem isso o resultado depende do
# ambiente: no Spark 4.2 a conf `spark.sql.execution.pythonUDF.arrow.enabled` vem ligada, e com
# `pyarrow` instalado o mesmo `F.udf` passa a trafegar em Arrow (ver a nota abaixo da célula).
slug = F.udf(slug_py, StringType(), useArrow=False)
print("spark.sql.execution.pythonUDF.arrow.enabled =",
      spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"))
src_udf = lambda: ev().select("actor_login")  # noqa: E731  (2 milhões de linhas)
t_native = time_it(lambda: src_udf().select(F.max(F.regexp_replace(F.lower("actor_login"), r"\[bot\]", "")))
                   .collect(), repeat=3, warmup=1, label="função nativa")
t_py = time_it(lambda: src_udf().select(F.max(slug("actor_login"))).collect(), repeat=3, warmup=1,
               label="UDF Python clássica (pickle, linha a linha)")
udf_timings = [t_native, t_py]
try:
    import pandas as pd  # noqa: F401
    import pyarrow  # noqa: F401

    @F.pandas_udf(StringType())
    def slug_pd(s: "pd.Series") -> "pd.Series":
        return s.str.lower().str.replace("[bot]", "", regex=False)

    udf_timings.append(time_it(lambda: src_udf().select(F.max(slug_pd("actor_login"))).collect(), repeat=3,
                               warmup=1, label="pandas UDF (Arrow, vetorizada)"))
except ImportError as exc:
    print(f"⚠️ pandas UDF não medida: {exc.name} não está instalado neste ambiente "
          "(pandas UDF e toPandas() exigem `pandas` e `pyarrow`).")
print(compare(udf_timings))
print("\nplano da UDF clássica:", grep_plan(src_udf().select(slug("actor_login")), "BatchEvalPython", "ArrowEvalPython"))
if len(udf_timings) == 3:
    print("plano da pandas UDF  :", grep_plan(src_udf().select(slug_pd("actor_login")), "BatchEvalPython",
                                              "ArrowEvalPython"))

# %% [markdown]
# A tabela acima é a medição; a prova estrutural é o plano: a UDF clássica aparece como
# `BatchEvalPython` e a pandas UDF como `ArrowEvalPython` — as duas são a fronteira JVM ↔ Python,
# que a função nativa não tem. A diferença para a nativa cresce com a complexidade e o volume.
#
# **Achado deste ambiente (vale ouro em entrevista).** Em Spark 4.2 a conf
# `spark.sql.execution.pythonUDF.arrow.enabled` vem `true` (a célula imprime). Logo, o MESMO
# `F.udf(...)` muda de caminho conforme o `pyarrow` esteja ou não instalado: sem ele, pickle; com
# ele, Arrow. Ao instalar `pandas`/`pyarrow` para medir a pandas UDF, esta célula — que antes
# rodava — **passou a travar**: a UDF com Arrow não terminava sobre a leitura direta da tabela
# completa (reproduzido isoladamente; em amostras e com um filtro no plano ela termina). Não
# investiguei a causa raiz. A correção aqui foi declarar a intenção: `useArrow=False` para medir o
# caminho clássico. Lições: (1) fixe as versões e as confs de que o resultado depende; (2) um
# upgrade de dependência muda o plano físico sem mudar uma linha do seu código; (3) job que
# "ficou lento/travou depois do upgrade" se investiga comparando o plano e as confs efetivas.
#
# > 🎤 **Resposta de 30 s:** "Ordem de preferência: função nativa do Spark, depois pandas UDF
# > (Arrow, vetorizada), por último UDF Python linha a linha. A UDF Python serializa cada linha
# > para outro processo, quebra o codegen e o pushdown e no Databricks não roda no Photon."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Spark 3.5+ tem **UDF Python otimizada com Arrow** (`@udf(useArrow=True)` ou
#   `spark.sql.execution.pythonUDF.arrow.enabled`) — mesma API, transporte em Arrow. Na sessão
#   deste notebook (Spark 4.2) a conf já vem ligada — ver o achado acima.
# - `mapInPandas` / `applyInPandas`: lógica arbitrária por lote ou por grupo (ex.: modelo por
#   cliente). `applyInPandas` carrega o grupo inteiro na memória do worker Python — skew aqui é OOM.
# - UDF em Scala/Java (JVM) evita a serialização, mas também é caixa-preta para o otimizador.
# - Funções de alta ordem do SQL (`transform`, `filter`, `aggregate` em arrays) substituem
#   muitas UDFs de lista.
# </details>
#
# **Trade-offs:** UDF é legítima para lógica que não existe nativa (biblioteca Python, modelo de
# ML). Isole, teste à parte e meça.

# %% [markdown]
# ## 12. `collect()` / `toPandas()` e OOM no driver
#
# **O que é.** `collect()` traz **todas** as linhas para a memória do driver (um único processo).
# `toPandas()` faz o mesmo e ainda converte para pandas.
#
# **Por que importa.** O driver tem alguns GB; a tabela, centenas. É o OOM mais comum de quem
# vem do pandas. O Spark tem uma proteção: `spark.driver.maxResultSize` (padrão 1 GB) aborta a
# action se o total serializado passar do limite — erro claro em vez de driver morto. Neste
# notebook o limite foi baixado para 128 MB para provocar o erro com segurança.

# %%
try:
    ev().collect()
except Exception as exc:  # noqa: BLE001 — queremos mostrar a mensagem do Spark
    msg = str(exc)
    print(type(exc).__name__, "→", msg[msg.find("Total size"):][:160])

sample = ev().select("actor_login", "type").limit(5).collect()  # alternativas seguras
print("limit+collect:", len(sample), "linhas")
first_rows = [r.type for _, r in zip(range(3), ev().select("type").toLocalIterator(), strict=False)]
print("toLocalIterator (partição por partição):", first_rows)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "`collect` e `toPandas` trazem tudo para o driver, que é uma
# > máquina só. Eu agrego ou filtro antes, uso `limit`/`take` para inspeção, `toLocalIterator`
# > se preciso iterar, e grava-se o resultado grande em tabela — nunca no driver. O
# > `maxResultSize` é a rede de segurança, não a solução."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Outras fontes de OOM no driver: broadcast grande (o driver coleta antes de enviar), plano
#   gigante (milhares de colunas/uniões), muitos arquivos pequenos (lista de arquivos e log Delta
#   enorme), `display()` de resultado grande.
# - OOM no **executor** é outra coisa: partição grande demais (skew), `explode`, janela sem
#   partição, pandas UDF com grupo enorme. Remédio: mais partições, salting, memória/core.
# - `toPandas()` com Arrow (`spark.sql.execution.arrow.pyspark.enabled`) é muito mais rápido,
#   mas não muda o limite de memória.
# </details>
#
# **Trade-offs:** subir `maxResultSize`/memória do driver resolve o sintoma uma vez; o padrão
# certo é não trazer volume para o driver.

# %% [markdown]
# ## 13. Spill para disco
#
# **O que é.** Quando uma task precisa de mais memória de execução do que tem (sort, hash de
# agregação, join), ela despeja parte dos dados em disco e continua — *spill*. Não falha, mas
# fica lenta: serializa, grava, lê de novo e faz merge.
#
# **Por que importa.** Spill é o sintoma de partição grande demais (poucas partições ou skew).
# Na Spark UI aparece como **Spill (Memory)** e **Spill (Disk)** no stage.
#
# **Como funciona.** Ordenar o dia inteiro **com** `payload` dentro de cada partição, com 2 × 64
# partições de shuffle. Gravamos no formato `noop` (executa tudo e descarta a saída — mede o
# processamento sem I/O de escrita).

# %%
spill_rows = []
with spark_conf(spark, {"spark.sql.adaptive.enabled": "false"}):
    for parts in (2, 64):
        with spark_conf(spark, {"spark.sql.shuffle.partitions": str(parts)}), \
                job_group(spark, f"s13-{parts}"), stopwatch() as sw_sp:
            (ev().select("id", "actor_login", "payload").repartition("actor_login")
             .sortWithinPartitions("actor_login", "id").write.format("noop").mode("overwrite").save())
        st = ui.heaviest_shuffle_stage(f"s13-{parts}")
        spill_rows.append({"partições": parts, "tempo (s)": round(sw_sp.seconds, 1),
                           "shuffle lido MB": round(st["shuffleReadBytes"] / 1e6),
                           "spill memória MB": round(st["memoryBytesSpilled"] / 1e6),
                           "spill disco MB": round(st["diskBytesSpilled"] / 1e6)})
print(as_table(spill_rows))

# %% [markdown]
# Com 2 partições, cada task recebe centenas de MB e derrama; com 64, cada uma cabe na memória
# e o spill é zero. (O tempo aqui é de **uma** execução de cada — a prova são os bytes de spill,
# que não dependem do ruído da máquina.) *Spill (Memory)* é o tamanho desserializado do que foi despejado; *Spill
# (Disk)* é o mesmo dado serializado e comprimido no disco — por isso é menor.
#
# > 🎤 **Resposta de 30 s:** "Spill é a task sem memória de execução despejando em disco — não
# > quebra, mas fica lenta. Vejo nas métricas do stage. O remédio é partição menor: mais
# > `shuffle.partitions` ou AQE, tratar skew, ou mais memória por core (VM memory-optimized).
# > Aumentar memória sem olhar o tamanho da partição só adia o problema."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Memória unificada**: `spark.memory.fraction` (0,6 do heap menos 300 MB) é dividida entre
#   execução e armazenamento (cache); execução pode tomar o espaço do cache. Por isso cache
#   grande causa spill.
# - Memória **por core** é o que importa: executor de 32 GB com 8 cores = ~4 GB por task.
# - Spill de shuffle write (*sort-based shuffle*) e de agregação (`HashAggregate` cai para
#   sort-based — "number of sort fallback tasks" nas métricas SQL).
# - No Databricks: discos locais NVMe aceleram spill e shuffle; VMs `L`/`E` na Azure.
# </details>
#
# **Trade-offs:** mais partições reduzem spill até o ponto em que o overhead por task (§2)
# volta a dominar.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# **Por que `local[4]` não reproduz tudo.** Uma JVM faz papel de driver e executor (memória
# compartilhada); shuffle é disco local, não rede; não há latência de object storage (ADLS),
# que é o que torna small files e listagem caros de verdade; não há Photon nem disk cache; e o
# laptop está dividido com outros processos. O que se reproduz bem é o **mecanismo** — plano,
# número de tasks, registros por task, arquivos lidos, spill. Esses são os números confiáveis.
#
# **Photon.** Motor de execução vetorizado em C++ que substitui operadores da JVM (scan, filtro,
# agregação, join, escrita Delta). Ligado por padrão em SQL warehouses e serverless; em compute
# clássico é uma opção com DBU mais caro — compensa em carga SQL/DataFrame pesada, não em código
# dominado por UDF Python (que não roda no Photon). No plano/Query Profile os operadores aparecem
# como `Photon...`; o que não é suportado volta para a JVM.
#
# **Liquid Clustering e Predictive Optimization.**
#
# ```sql
# -- Tabela nova: clustering em vez de partição + Z-order
# CREATE TABLE main.gold.gh_events_daily (...) CLUSTER BY (repo_id, event_date);
# -- Ou deixar o Databricks escolher as chaves pelo histórico de consultas (UC managed + PO)
# ALTER TABLE main.gold.gh_events_daily CLUSTER BY AUTO;
# -- Trocar as chaves sem reescrever o que já existe
# ALTER TABLE main.gold.gh_events_daily CLUSTER BY (actor_login);
# -- Predictive Optimization: OPTIMIZE, VACUUM e ANALYZE automáticos em tabelas UC managed
# ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;
# SELECT * FROM system.storage.predictive_optimization_operations_history;  -- o que ele fez
# ```
#
# Predictive Optimization vem habilitada por padrão em contas mais novas (conferir no account
# console); roda como serverless e é cobrada como tal. Com ela, o "agendar OPTIMIZE/VACUUM" deixa
# de ser tarefa do pipeline.
#
# **Escritas otimizadas e shuffle automático.**
#
# ```python
# spark.conf.set("spark.sql.shuffle.partitions", "auto")  # auto-optimized shuffle (Databricks)
# spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true',"
#           " 'delta.autoOptimize.autoCompact'='true')")
# ```
#
# **Dimensionamento de cluster (Azure Databricks).**
#
# | Decisão | Regra prática |
# |---|---|
# | Workers × tamanho | Poucos workers grandes reduzem shuffle pela rede; muitos pequenos dão paralelismo e tolerância a perda de spot |
# | Memória por core | ETL com join/agregação pesados: VMs memory-optimized (série E); scan/CPU: compute-optimized (série F); muito shuffle/spill: discos NVMe (série L) |
# | Núcleos por executor | No Databricks, 1 executor por worker usando todos os cores; memória por task ≈ memória do executor × 0,6 ÷ cores |
# | Partições | Total de tasks de um stage = múltiplo de cores; partição de 100–200 MB |
# | Driver | Maior só se houver `collect`, broadcast grande ou muitos arquivos/plano grande |
# | Autoscaling | Bom para carga variável; em job batch curto, o tempo de subir nó pode custar mais que economiza |
# | Serverless | Sem dimensionamento: o Databricks escolhe e escala; paga-se por DBU (notebook 15) |
#
# **Diagnóstico no Databricks**: Spark UI do job run (mesma REST API usada aqui), *Query
# Profile* (SQL/serverless), `system.query.history` e as métricas do compute (notebook 15).

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. O que é lazy evaluation e qual a vantagem?**
# <details><summary>Resposta</summary>Transformações só montam o plano; a action executa. O
# Catalyst otimiza o plano inteiro (pushdown, pruning, escolha de join) antes de rodar. Custo:
# reusar o DataFrame recalcula tudo (§1).</details>
#
# **2. Diferença entre transformação narrow e wide? Onde começa um stage novo?**
# <details><summary>Resposta</summary>Narrow: cada partição de saída depende de uma de entrada
# (filter, select) — mesmo stage. Wide: precisa reunir chaves de todas as partições (groupBy,
# join) — exige shuffle, que aparece como `Exchange` e abre stage novo (§1).</details>
#
# **3. Um job travou em 199/200 tasks. O que você investiga?**
# <details><summary>Resposta</summary>Skew. Na Spark UI: Summary Metrics do stage (max ≫
# mediana em duração e shuffle read), qual chave domina (`groupBy(key).count()`), chave nula.
# Conserto: AQE skew join, broadcast se a dimensão couber, salting nas chaves quentes, tratar a
# chave à parte (§5).</details>
#
# **4. Broadcast hash join × sort-merge join: quando cada um?**
# <details><summary>Resposta</summary>BHJ quando um lado cabe na memória dos executores
# (dimensão): sem shuffle do lado grande. SMJ para dois lados grandes. Automático abaixo de
# `autoBroadcastJoinThreshold` (10 MB), hint para forçar; AQE troca em runtime (§3b, §4).</details>
#
# **5. O que o AQE faz e o que ele não faz?**
# <details><summary>Resposta</summary>Faz: coalesce de partições, troca para broadcast, divisão
# de partições com skew em join — tudo a partir do tamanho real do shuffle. Não faz: nada sem
# shuffle (scan de small files), skew em janela, e mede skew em bytes, não em linhas (§3, §5).</details>
#
# **6. Como escolher `spark.sql.shuffle.partitions`?**
# <details><summary>Resposta</summary>Partições de ~100–200 MB, múltiplo do total de cores; na
# prática, valor alto + AQE coalescendo. 200 é padrão arbitrário (§2).</details>
#
# **7. Por que small files são um problema e como resolver?**
# <details><summary>Resposta</summary>Overhead por arquivo (listar, abrir, rodapé, entrada no
# log; no ADLS, requisições HTTP) domina. Prevenir (micro-lotes maiores, optimized writes, não
# over-particionar) e compactar (`OPTIMIZE`, auto compaction, Predictive Optimization) (§6).</details>
#
# **8. Partição, Z-order ou Liquid Clustering?**
# <details><summary>Resposta</summary>Partição: baixa cardinalidade, sempre filtrada, ≥ 1 GB por
# partição. Z-order: clustering por OPTIMIZE, não incremental. Liquid: incremental, chaves
# mutáveis, substitui os dois em tabela nova; `CLUSTER BY AUTO` no Databricks (§7, §8).</details>
#
# **9. Como provar que o filtro foi empurrado para a leitura?**
# <details><summary>Resposta</summary>`explain("formatted")`: `PushedFilters`, `PartitionFilters`,
# `ReadSchema`; e na métrica do scan, arquivos lidos × total. UDF ou cast na coluna quebram (§9).</details>
#
# **10. Quando usar cache?**
# <details><summary>Resposta</summary>Resultado caro reusado várias vezes na mesma sessão. Não em
# pipeline de uma passada: ocupa memória de execução (spill) e fica velho se a fonte muda.
# `unpersist` no fim (§10).</details>
#
# **11. Por que UDF Python é lenta e quais as alternativas?**
# <details><summary>Resposta</summary>Serializa linha a linha JVM ↔ Python, quebra codegen e
# pushdown, não roda no Photon. Alternativas: função nativa, funções de alta ordem, pandas UDF ou
# UDF com Arrow (§11).</details>
#
# **12. Spill e OOM: qual a diferença e como tratar?**
# <details><summary>Resposta</summary>Spill: task sem memória de execução despeja em disco e fica
# lenta. OOM no executor: partição grande demais para caber nem com spill (skew, explode). OOM no
# driver: `collect`/broadcast grande. Remédio comum: partições menores; no driver, não trazer
# volume (§12, §13).</details>

# %% [markdown]
# ## Resumo
#
# - **Leia o plano e a UI antes de mexer:** `Exchange` = shuffle = stage novo; max ≫ mediana nas
#   tasks = skew; Spill no stage = partição grande demais; `number of files read` = layout.
# - **Shuffle é o custo central:** broadcast quando um lado é pequeno, partições de 100–200 MB,
#   AQE ligado com `shuffle.partitions` alto.
# - **Skew:** AQE skew join resolve join (mede bytes!); janela e chave nula exigem salting ou
#   tratamento à parte; agregação simples já tem combine parcial.
# - **Layout Delta decide a leitura:** sem small files (OPTIMIZE), dado agrupado pela coluna do
#   filtro (Liquid > Z-order > partição só se ≥ 1 GB), só as colunas necessárias.
# - **Fique na JVM e fora do driver:** função nativa > pandas UDF > UDF Python; nada de
#   `collect` de volume; cache só com reuso real.

# %%
spark.stop()
