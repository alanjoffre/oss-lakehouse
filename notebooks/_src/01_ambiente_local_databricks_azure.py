# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 01 · Ambiente: Spark local, Databricks e Azure
#
# > Este notebook prova que o Spark + Delta do laptop é configurado de propósito (cada config tem motivo),
# > mostra como ele se compara ao Databricks e ao Azure Databricks, e diz com honestidade o que não dá para
# > reproduzir localmente.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Databricks e processamento de dados | §1–§5 (arquitetura do Spark), §7 (runtimes, compute, Free Edition, `dbutils`, Volumes) |
# | Microsoft Azure | §8 (workspace, VNet injection, Key Vault, Access Connector) |
# | Arquitetura de pipelines | §9 (paridade local × Databricks — o que muda entre ambientes) |
# | Python avançado | §5 (`get_spark()` e `Settings` — configuração por ambiente) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup
#
# Subir a sessão é o primeiro custo de qualquer job Spark: a JVM inicia, o Ivy resolve o JAR do Delta
# (do cache depois da 1ª vez) e o driver abre a UI. Medimos esse tempo.

# %%
import importlib.metadata as md
import inspect
import platform
import shutil
import sys
import time
from pathlib import Path

import pyspark
import requests

from oss_lakehouse.config import Settings, get_settings
from oss_lakehouse.spark import _on_databricks, get_spark

settings = get_settings()
DEMO = Path(settings.data_root) / "demo" / "01"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True, exist_ok=True)

t0 = time.perf_counter()
spark = get_spark("01")
startup_s = time.perf_counter() - t0
sc = spark.sparkContext
print(f"sessão pronta em {startup_s:.1f}s | Spark {spark.version} | master={sc.master} | app={sc.appName}")

# %% [markdown]
# ## 1. Driver, executor e cluster manager 🧪
#
# **O que é** — Uma aplicação Spark tem um **driver** (processo que roda o seu código Python/SQL, monta o plano
# e divide o trabalho em *tasks*) e **executors** (processos que executam as tasks sobre as partições do dado e
# guardam cache). Quem entrega máquinas para os executors é o **cluster manager** (*gerenciador de cluster*:
# Standalone, YARN, Kubernetes — no Databricks, o próprio plano de controle da plataforma).
#
# **Por que importa** — Quase todo problema de produção se explica por *onde* o código roda: `collect()` traz
# tudo para o driver (OOM no driver); UDF Python roda nos executors (serialização); um arquivo lido com
# `open()` existe no driver, não nos executors.
#
# **Como funciona**
#
# ```mermaid
# flowchart LR
#     subgraph Driver["Driver (seu código)"]
#         P["plano lógico → otimizador → plano físico"] --> S["stages → tasks"]
#     end
#     CM["Cluster manager<br/>(local / YARN / K8s / Databricks)"]
#     S -- pede recursos --> CM
#     CM --> E1["Executor 1<br/>tasks + cache"]
#     CM --> E2["Executor 2<br/>tasks + cache"]
#     S -- envia tasks --> E1 & E2
# ```
#
# **`local[N]`** é o modo de um processo só: driver e executor são **a mesma JVM**, com N *threads* de execução
# (N tasks em paralelo). `local[*]` usa todos os núcleos. Aqui `local[4]` (de `OSSLH_SPARK_MASTER`) — deixa
# CPU para os outros processos da máquina. Abaixo, a prova: um job com 8 partições, e quantas tasks rodaram ao
# mesmo tempo.

# %%
df = spark.range(0, 20_000_000, numPartitions=8)
print(f"partições={df.rdd.getNumPartitions()}  paralelismo padrão={sc.defaultParallelism}")
for rodada in (1, 2):
    sc.setJobDescription(f"01 §1 soma de 20M, rodada {rodada}")  # rótulo que aparece na Spark UI (§4)
    t0 = time.perf_counter()
    total = df.selectExpr("sum(id) AS s").first()["s"]
    print(f"rodada {rodada}: soma={total:,}  tempo de parede={time.perf_counter() - t0:.2f}s")
sc.setJobDescription(None)

# %% [markdown]
# A UI do Spark expõe uma API REST (a mesma que a página web usa). Ela mostra que em `local[N]` existe **um**
# executor — o `driver` — com N núcleos:

# %%
ui = sc.uiWebUrl
app_id = sc.applicationId
api = f"{ui}/api/v1/applications/{app_id}"
for ex in requests.get(f"{api}/executors", timeout=5).json():
    print(f"executor id={ex['id']:<7} núcleos={ex['totalCores']}  memória p/ armazenamento={ex['maxMemory'] / 2**20:,.0f} MiB"
          f"  tasks concluídas={ex['completedTasks']}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "O driver roda o meu código, otimiza o plano e quebra o trabalho em stages e tasks;
# > os executors executam as tasks sobre as partições e guardam cache; o cluster manager aloca as máquinas.
# > Em `local[N]` tudo é uma JVM com N threads — por isso no laptop a memória que importa é a do driver.
# > No Databricks, o cluster manager é a plataforma e cada worker roda um executor."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Job → stage → task:** cada *action* (`count`, `write`) gera um job; o job é cortado em **stages** nas
#   fronteiras de *shuffle* (redistribuição de dados entre partições pela rede); cada stage tem uma task por
#   partição. Detalhe no notebook 09.
# - **Deploy mode:** em `client` o driver roda onde você chamou (laptop, notebook); em `cluster` roda dentro do
#   cluster. No Databricks o driver é sempre um nó do cluster.
# - **Dynamic allocation / autoscaling:** o número de executors varia com a fila de tasks. No Databricks é o
#   *autoscaling* do cluster (min/max workers) — está no `databricks.yml` deste repo.
# - **Por que não `local[*]` aqui:** com 5 processos Spark na mesma máquina, cada um pegaria todos os núcleos e
#   brigariam pelo CPU. Ver `scripts/spark_slot.sh`.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `local[N]` não tem rede entre executors: shuffle é cópia em disco local — rápido demais para representar
#   custo de cluster. Medidas de performance local mostram *tendência*, não número de produção.
# - Um driver só = ponto único de falha; em produção, driver grande demais costuma ser sintoma de `collect()`.

# %% [markdown]
# ## 2. Memória: o que dá para mudar depois que a sessão sobe 🧪
#
# **O que é** — `spark.driver.memory` define o *heap* (memória gerenciada) da JVM do driver. Dentro do heap, o
# Spark usa a **memória unificada** (`spark.memory.fraction`, padrão 0,6 do heap menos 300 MiB reservados),
# dividida dinamicamente entre **execução** (shuffle, join, sort) e **armazenamento** (cache).
#
# **Por que importa** — Configurações de JVM são **estáticas**: valem só na criação do processo. Mudar
# `spark.driver.memory` depois que a sessão existe não faz nada — e o Spark não reclama alto. Muita gente
# "aumenta a memória" num notebook e não percebe que nada mudou.
#
# **Como funciona** — `getOrCreate()` devolve a sessão **existente** se houver uma; configs de SQL são aplicadas,
# configs estáticas são ignoradas. A prova: pedir 8g e medir o heap real da JVM.

# %%
def heap_gib() -> float:
    return sc._jvm.java.lang.Runtime.getRuntime().maxMemory() / 2**30


print(f"ANTES   spark.driver.memory = {spark.conf.get('spark.driver.memory'):<4} heap real da JVM = {heap_gib():.2f} GiB")

from pyspark.sql import SparkSession  # noqa: E402

again = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()
print(f"DEPOIS  spark.driver.memory = {spark.conf.get('spark.driver.memory'):<4} heap real da JVM = {heap_gib():.2f} GiB"
      f"   (mesma sessão? {again is spark})")
print(f"spark.memory.fraction = {spark.conf.get('spark.memory.fraction', '0.6 (padrão)')}")

# E com spark.conf.set? Para configs do core, o Spark recusa explicitamente — pelo menos esse caminho avisa.
try:
    spark.conf.set("spark.driver.memory", "4g")
except Exception as exc:
    print(f"spark.conf.set recusado: {type(exc).__name__}: {str(exc).splitlines()[0][:110]}")

# %% [markdown]
# Repare no pior detalhe: depois do "8g", **a config passa a dizer 8g, mas o heap continua 2 GiB**. A JVM já
# existia; o valor só foi gravado no dicionário de configuração da sessão. Conferir config não prova nada —
# confira o recurso real (heap, executors na Spark UI).
#
# > 🎤 **Resposta de 30 s:** "Memória de driver e executor é config estática: precisa estar no `spark-submit`,
# > na config do cluster ou no builder **antes** de a JVM subir. Num notebook do Databricks a sessão já existe,
# > então isso se ajusta no cluster ou no tipo de instância, não com `spark.conf.set`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Memória fora do heap:** `spark.executor.memoryOverhead` (padrão 10%, mínimo 384 MiB) cobre o processo
#   Python das UDFs, buffers de rede e Arrow. *Container killed by YARN/K8s for exceeding memory limits* é
#   overhead, não heap.
# - **OOM no driver:** `collect()`, `toPandas()`, broadcast grande demais, plano gigante (milhares de colunas
#   ou `union` em laço). **OOM no executor:** partição grande demais (skew), agregação com muitas chaves.
# - **Configs dinâmicas** (`spark.sql.shuffle.partitions`, AQE, `autoBroadcastJoinThreshold`) podem mudar por
#   sessão a qualquer momento com `spark.conf.set`.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Mais heap não é sempre melhor: heaps muito grandes (> ~64 GB) aumentam pausas de GC; melhor mais executors
#   menores.
# - Cache "por via das dúvidas" rouba memória de execução; cache só o que é reusado (notebook 09).

# %% [markdown]
# ## 3. Delta Lake local: o JAR tem de casar com o Spark 🧪
#
# **O que é** — O Delta Lake tem duas partes: o pacote Python `delta-spark` (API `DeltaTable`) e o **JAR** que
# roda dentro da JVM (o *transaction log*, MERGE, time travel). O JAR é publicado por versão de Spark e de Scala.
#
# **Por que importa** — JAR de Delta compilado para outro Spark sobe normalmente e quebra na primeira operação
# com `NoSuchMethodError` ou `ClassNotFoundException` — erro feio, longe da causa. No Databricks isso não existe:
# o runtime já traz o Delta certo (e **não** se instala `delta-spark` num cluster).
#
# **Como funciona** — `configure_spark_with_delta_pip` lê a versão do `delta-spark` e do `pyspark` instalados e
# monta a coordenada Maven `io.delta:delta-spark_<spark major.minor>_<scala>:<versão do delta>`, que o Ivy
# baixa na 1ª vez e guarda em cache.

# %%
print(f"Python {platform.python_version()} | pyspark {pyspark.__version__} | delta-spark {md.version('delta-spark')}")
print(f"Scala da JVM: {sc._jvm.scala.util.Properties.versionNumberString()} | "
      f"Java: {sc._jvm.java.lang.System.getProperty('java.version')}")
print("spark.jars.packages =", spark.conf.get("spark.jars.packages"))
print("spark.sql.extensions =", spark.conf.get("spark.sql.extensions"))

# %% [markdown]
# Prova de que as extensões estão ativas: criar uma tabela Delta, fazer um MERGE (só existe com o JAR) e ler o
# histórico. Tudo em `data/demo/01/` — nunca nas tabelas compartilhadas.

# %%
from delta.tables import DeltaTable  # noqa: E402

path = str(DEMO / "smoke_delta")
spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING").write.format("delta").save(path)
updates = spark.createDataFrame([(2, "B"), (3, "c")], "id INT, v STRING")
(DeltaTable.forPath(spark, path).alias("t")
 .merge(updates.alias("u"), "t.id = u.id")
 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
spark.read.format("delta").load(path).orderBy("id").show()
(DeltaTable.forPath(spark, path).history()
 .select("version", "operation", "operationMetrics.numTargetRowsUpdated", "operationMetrics.numTargetRowsInserted")
 .orderBy("version").show(truncate=False))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Localmente, o Delta é um JAR que precisa casar com a versão do Spark e do Scala;
# > o `configure_spark_with_delta_pip` resolve a coordenada certa a partir do pip. No Databricks o Delta vem no
# > runtime, então a versão do Delta é a do DBR — e eu nunca instalo `delta-spark` ou `pyspark` no cluster."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Duas configs obrigatórias:** `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` (sintaxe SQL:
#   `MERGE`, `VACUUM`, `DESCRIBE HISTORY`) e `spark.sql.catalog.spark_catalog=…DeltaCatalog` (o catálogo
#   entende tabelas Delta).
# - **Ambiente offline/corporativo:** sem acesso ao Maven Central, aponta-se `spark.jars.ivySettings` para um
#   espelho interno (Artifactory/Nexus) ou passa-se o JAR em `spark.jars`.
# - **Versões no Databricks não são as do OSS:** o DBR traz um Delta próprio, com recursos que chegam antes
#   (ex.: Predictive Optimization). Compare features pela documentação do runtime, não pelo número de versão.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Se só precisa **ler** Delta em Python sem JVM, `deltalake` (delta-rs), DuckDB ou Polars são mais leves.
# - O 1º start baixa ~10 JARs: em CI, faça cache do diretório do Ivy (está no `.github/workflows/ci.yml`).

# %% [markdown]
# ## 4. A Spark UI 🧪
#
# **O que é** — Interface web do driver (porta 4040; 4041, 4042… se a anterior estiver ocupada) com jobs, stages,
# tasks, armazenamento, ambiente (todas as configs) e o plano SQL de cada consulta.
#
# **Por que importa** — É a primeira ferramenta de diagnóstico: skew aparece como uma task muito mais lenta que
# a mediana; spill aparece como "Spill (Disk)"; shuffle grande, como "Shuffle Read". No Databricks a mesma UI
# está em *Compute → cluster → Spark UI* e, nas execuções de job, no link da task.
#
# **Como funciona** — A API REST da UI permite ler as mesmas métricas por código (útil para testes de
# performance automatizados):

# %%
jobs = requests.get(f"{api}/jobs", timeout=5).json()
stages = requests.get(f"{api}/stages", timeout=5).json()
print(f"UI: {ui}  (porta muda se outra sessão já ocupa a 4040)")
print(f"jobs desde o início da sessão: {len(jobs)}  stages: {len(stages)}")

for soma in sorted((j for j in jobs if str(j.get("description", "")).startswith("01 §1")), key=lambda j: j["jobId"]):
    print(f"\n{soma['description']}: job {soma['jobId']} {soma['status']} stages={soma['stageIds']} tasks={soma['numTasks']}")
    for sid in soma["stageIds"]:
        st = requests.get(f"{api}/stages/{sid}", timeout=5).json()[0]
        print(f"  stage {sid}: tasks={st['numTasks']:<2} tempo somado das tasks={st['executorRunTime']:>4} ms  "
              f"shuffle escrito={st['shuffleWriteBytes']:,} B  lido={st['shuffleReadBytes']:,} B")

# %% [markdown]
# Como ler a saída acima:
#
# - **Uma ação, dois jobs.** Com o AQE ligado, cada *query stage* é submetido separadamente: o 1º job faz a
#   agregação parcial nas 8 partições e grava um shuffle minúsculo (uma linha por partição); o 2º lê esse shuffle
#   numa única task e fecha a soma. O stage de 8 tasks com 0 ms dentro do 2º job é o mesmo trabalho já feito,
#   reaproveitado (*skipped*).
# - **1ª rodada × 2ª rodada.** Mesmo plano, mesmos dados — compare o tempo de parede da §1 e o tempo das tasks
#   aqui: a 1ª execução paga geração de código e aquecimento da JVM (JIT). Benchmark que mede só a 1ª execução
#   mede o aquecimento, não a consulta (notebook 09).
#
# > 🎤 **Resposta de 30 s:** "Na Spark UI eu olho primeiro a aba SQL para ver o plano físico e onde está o
# > tempo; depois o stage mais lento e a distribuição de duração das tasks — máximo muito acima da mediana é
# > skew; spill em disco é falta de memória por partição. Ela tem API REST, então dá para automatizar."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **History Server:** a UI morre com o driver; para jobs concluídos, o *event log*
#   (`spark.eventLog.enabled`) alimenta o History Server. No Databricks, o histórico do cluster cumpre esse papel
#   por um período, e as *system tables* guardam custo e execução (notebook 15).
# - **Serverless:** não expõe a Spark UI clássica; o diagnóstico é pelo *query profile* e pelo histórico de
#   consultas.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - A UI mostra uma execução; para tendência (o job está ficando mais lento a cada dia?) use métricas
#   persistidas, não a UI.

# %% [markdown]
# ## 5. `get_spark()` linha a linha 🧪
#
# **O que é** — A função única do projeto para obter a sessão (`src/oss_lakehouse/spark.py`). Todo notebook,
# teste e comando do CLI passa por ela.
#
# **Por que importa** — Configuração espalhada é configuração divergente: um notebook com `shuffle.partitions=200`
# e outro com 8 dão resultados de performance incomparáveis. E o mesmo código precisa rodar no Databricks, onde a
# sessão **já existe** e não deve ser recriada.
#
# **Como funciona**

# %%
print(inspect.getsource(get_spark))

# %% [markdown]
# | Config | Valor aqui | Por quê |
# |---|---|---|
# | `master` | `local[4]` | 4 threads; deixa CPU para outros processos. Vem de `OSSLH_SPARK_MASTER` |
# | `spark.sql.extensions` + `spark_catalog` | Delta | habilita SQL e catálogo do Delta (§3) |
# | `spark.driver.memory` | `2g` | estática (§2); 2 GB bastam para 3 horas de GH Archive |
# | `spark.sql.shuffle.partitions` | `8` | o padrão 200 gera 200 tasks minúsculas no laptop; no Databricks o AQE ajusta sozinho |
# | `spark.sql.session.timeZone` | `UTC` | o GH Archive é UTC; fuso implícito da máquina é bug clássico em `to_date` |
# | `spark.ui.showConsoleProgress` | `false` | a barra de progresso polui a saída do notebook versionado |
# | `spark.databricks.delta.schema.autoMerge.enabled` | `false` | coluna nova na fonte não entra calada na tabela (ADR 0003) |
# | `_on_databricks()` | `DATABRICKS_RUNTIME_VERSION` | no Databricks devolve a sessão ativa: nada de builder |
#
# A tabela acima foi conferida contra a sessão ativa:

# %%
for k in ["spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions", "spark.sql.session.timeZone",
          "spark.ui.showConsoleProgress", "spark.databricks.delta.schema.autoMerge.enabled",
          "spark.sql.adaptive.enabled"]:
    print(f"{k:<50} {spark.conf.get(k)}")
print(f"heap real da JVM                                   {heap_gib():.2f} GiB  ← o 8g acima é o resíduo da §2")
print(f"\n_on_databricks() = {_on_databricks()}   settings.env = {settings.env!r}")

# %% [markdown]
# A outra metade da paridade é o `Settings`: o código do pipeline nunca escreve um caminho literal. A mesma
# chamada aponta para a pasta local ou para o ADLS Gen2 — só muda a variável de ambiente:

# %%
local = Settings()
nuvem = Settings(env="databricks", data_root="abfss://lake@stosslakehousedev.dfs.core.windows.net")
for nome, s in [("local", local), ("databricks", nuvem)]:
    print(f"{nome:<11} bronze → {s.path('bronze', 'gh_events')}")
    print(f"{'':<11} checkpoint → {s.checkpoint('bronze_gh_events')}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Tenho uma função só para a sessão e uma classe de configuração só para caminhos.
# > Local ela monta a sessão com Delta e configs para laptop; no Databricks ela devolve a sessão que já existe.
# > Os caminhos vêm de variáveis de ambiente, então o mesmo wheel roda nos dois lugares — o job do bundle só
# > define `OSSLH_DATA_ROOT`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que não `spark.conf.set` no código do pipeline:** config de performance pertence ao ambiente (cluster,
#   job), não à lógica. No Databricks, `spark.sql.shuffle.partitions` fixo atrapalha o AQE (*Adaptive Query
#   Execution*, que reotimiza o plano com estatísticas reais em tempo de execução) — por isso só fixamos local.
# - **12-factor app:** configuração por ambiente vem do ambiente (variáveis), não do código. `pydantic-settings`
#   valida tipo e valores permitidos (`env: Literal["local", "databricks"]`).
# - **Teste:** o `conftest.py` sobe uma sessão com `local[2]` e 2 partições de shuffle, numa pasta temporária —
#   a mesma função, outra configuração.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Detectar Databricks por variável de ambiente é simples, mas acopla; com Databricks Connect a sessão é
#   remota e o código local não vê `DATABRICKS_RUNTIME_VERSION` — aí se usa `DatabricksSession.builder`.

# %% [markdown]
# ## 6. Spark Connect (conceito) 🧪/☁️
#
# **O que é** — Arquitetura **cliente-servidor** do Spark (desde o 3.4, madura no 4.x): o cliente (Python, Scala,
# Go…) monta o plano lógico e o envia por **gRPC** (protocolo de chamada remota) ao servidor, que executa. O
# cliente não tem JVM.
#
# **Por que importa** — É a base do **Databricks Connect** (rodar código do IDE local num cluster/serverless
# remoto) e do **serverless** do Databricks. Muda o que é permitido: no Connect não existe `sparkContext`, nem RDD,
# nem `_jvm` — o código deste notebook que usa `sc._jvm` **não** rodaria via Connect.
#
# **Como funciona**
#
# ```mermaid
# flowchart LR
#     C["Cliente fino<br/>(pyspark-client / databricks-connect)"] -- "plano lógico (protobuf, gRPC)" --> S["Servidor Spark Connect<br/>(driver no cluster)"]
#     S -- "resultado em Arrow" --> C
#     S --> X["Executors"]
# ```

# %%
from pyspark.sql.utils import is_remote  # noqa: E402

print(f"sessão é Spark Connect? {is_remote()}  | classe: {type(spark).__module__}.{type(spark).__name__}")
print(f"tem sparkContext? {hasattr(spark, 'sparkContext')} (no Connect, acessar spark.sparkContext levanta erro)")

# %% [markdown]
# ☁️ Com Databricks Connect (não instalado aqui — exige workspace):
#
# ```python
# from databricks.connect import DatabricksSession
# spark = DatabricksSession.builder.serverless().getOrCreate()   # ou .clusterId("...")
# spark.read.table("samples.nyctaxi.trips").limit(5).show()      # o plano roda no Databricks
# ```
#
# > 🎤 **Resposta de 30 s:** "Spark Connect separa cliente e servidor: o cliente manda o plano por gRPC e recebe
# > o resultado em Arrow. É o que permite o Databricks Connect no IDE e o serverless. A consequência prática é que
# > RDD e `sparkContext` não existem nesse modo — código novo deve usar só a API de DataFrame."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Isolamento:** várias aplicações no mesmo servidor sem uma derrubar a outra (OOM no cliente não mata o
#   driver); atualização de servidor sem mudar o cliente.
# - **Versões:** Databricks Connect tem de casar com o DBR (ex.: `databricks-connect` 17.3 ↔ DBR 17.3).
# - **UDFs Python** continuam possíveis: o cliente serializa a função e o servidor a executa num processo Python.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Bibliotecas antigas que dependem de RDD/`_jvm` não funcionam via Connect nem no serverless.

# %% [markdown]
# ## 7. Databricks: runtime, compute, Free Edition e utilidades ☁️
#
# **O que é** — Databricks é a plataforma gerenciada sobre Spark + Delta + Unity Catalog. O **Databricks Runtime
# (DBR)** é a imagem versionada do cluster (Spark, Delta, Python, bibliotecas). **LTS** (*long-term support*) é a
# versão com 3 anos de correções — a escolha padrão para produção.
#
# **Por que importa** — A versão do runtime define Spark, Delta, Python e o que existe (ex.: VARIANT). Escolher o
# tipo de compute errado é o maior desperdício de custo em Databricks.
#
# **Como funciona** — Runtimes suportados em out/2026 (fonte:
# [Databricks Runtime release notes](https://docs.databricks.com/aws/en/release-notes/runtime/), consultada em
# 05/10/2026):
#
# | DBR | Spark | Status |
# |---|---|---|
# | 19 | 4.2.0 | mais novo (jun/2026) — mesma versão de Spark deste ambiente local |
# | 18 | 4.1.0 | GA; vira LTS quando o 19 ficar GA (novo ciclo Beta → GA → LTS de 2026) |
# | 17.3 LTS | 4.0.0 | LTS até out/2028 — o que o `databricks.yml` usa |
# | 16.4 LTS / 15.4 LTS | 3.5.x | LTS até 2028 / 2027 |
#
# **Tipos de compute**
#
# | Compute | Para quê | Cobrança | Observação |
# |---|---|---|---|
# | **All-purpose** (interativo) | notebooks, exploração | DBU mais cara, enquanto ligado | auto-terminate obrigatório; nunca para job agendado |
# | **Job compute** | jobs agendados (Lakeflow Jobs) | DBU de job (mais barata); sobe e morre com a execução | o padrão para pipeline em cluster clássico |
# | **Serverless** (notebooks, jobs, pipelines) | tudo acima sem gerenciar cluster | por uso; início em segundos | sem Spark UI clássica, sem RDD, sem init scripts; o único da Free Edition |
# | **SQL warehouse** | SQL/BI, dashboards, Genie | por tamanho (2X-Small…4X-Large) | Photon sempre; serverless, pro ou classic |
#
# **Databricks Free Edition** (substituiu a Community Edition em 2025). Limites oficiais
# ([Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations),
# atualizada em 29/09/2026):
#
# - só **serverless** (notebooks, jobs, pipelines), com tamanho e uso limitados; estourou a cota diária, o compute
#   para até o dia seguinte;
# - **1 SQL warehouse**, tamanho 2X-Small;
# - **até 5 tasks de job simultâneas** por conta; **1 pipeline ativo** por tipo;
# - 1 workspace e 1 metastore; sem console de conta, sem SSO/SCIM, sem rede privada;
# - Model Serving sem GPU e sem *provisioned throughput*; 1 endpoint de AI/Vector Search;
#   até 3 Databricks Apps (param após 24 h);
# - sem R e Scala; **uso não comercial**; sem SLA.
#
# Consequência para este repo: o job do bundle (job cluster) **não roda** na Free Edition como está; lá, troca-se
# o `job_cluster_key` por `environment_key` (serverless) — ver notebook 13.
#
# **Notebooks × arquivos** — no workspace há notebooks (formato próprio, ou `.py` com o cabeçalho
# `# Databricks notebook source`) e **arquivos de workspace** (`.py`, `.yml`, `.sql` comuns). Lógica reaproveitável
# vai em módulo/wheel, não em `%run` de notebook: módulo se testa, `%run` não.
#
# **Git folders** (antigo *Repos*): clone de um repositório Git dentro do workspace, com branch, commit, pull e
# push pela UI. Bom para desenvolver; **produção não deve rodar de Git folder pessoal** — deploy é pelo bundle.
#
# **Volumes** (Unity Catalog): área governada para **arquivos não tabulares** (landing, CSV, modelos, wheels),
# com caminho `/Volumes/<catálogo>/<schema>/<volume>/…` e permissão por `GRANT`. Substitui o DBFS root e os mounts,
# que a Databricks hoje desaconselha.
#
# **`dbutils`** — utilitários do notebook (☁️, não existe local):
#
# ```python
# dbutils.fs.ls("/Volumes/oss_dev/landing/gharchive/")              # sistema de arquivos (Volumes, abfss://)
# dbutils.fs.cp("/Volumes/.../a.json.gz", "/Volumes/.../b.json.gz")
# token = dbutils.secrets.get(scope="kv-oss", key="github-token")   # valor aparece como [REDACTED] na saída
# dbutils.widgets.text("data_ref", "2026-10-01")                     # parâmetro do notebook / da task do job
# data_ref = dbutils.widgets.get("data_ref")
# dbutils.jobs.taskValues.set("linhas_bronze", 92_000)               # passa valor para a próxima task do job
# ```
#
# Local, o equivalente é deliberadamente simples: segredos por variável de ambiente (`OSSLH_GITHUB_TOKEN`,
# lido pelo `Settings`), parâmetros por argumento do CLI, arquivos com `pathlib`.
#
# > 🎤 **Resposta de 30 s:** "Para produção eu uso job compute ou serverless, nunca all-purpose; runtime LTS;
# > código em wheel deployado por bundle, não notebook em Git folder pessoal. Arquivo bruto em Volume, segredo em
# > secret scope apoiado no Key Vault. A Free Edition serve para estudar — só serverless, 1 warehouse 2X-Small,
# > 5 tasks simultâneas e uso não comercial."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Photon:** motor vetorizado em C++ do Databricks; acelera SQL/DataFrame (scan, join, agregação), não UDF
#   Python. Custa mais DBU por hora — vale quando reduz o tempo mais que o preço.
# - **Pools:** instâncias pré-aquecidas para reduzir o tempo de subida de job cluster (o serverless resolve
#   isso de outro jeito).
# - **Access mode:** *Standard* (antigo shared, vários usuários, isolamento de processo) × *Dedicated* (antigo
#   single user). Unity Catalog exige um dos dois; *No isolation* é legado.
# - **Cluster policies:** limitam o que o usuário pode criar (tamanho, tags obrigatórias, auto-terminate) —
#   FinOps na fonte (notebook 15).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Serverless: menos controle (sem init script, sem Spark UI clássica, versões geridas pela plataforma) em troca
#   de zero gestão e início rápido. Para cargas longas e estáveis, job cluster dimensionado pode sair mais barato.
# - All-purpose para job agendado: paga DBU interativa e compartilha recurso com humanos — evitar.

# %% [markdown]
# ## 8. Azure Databricks: workspace, rede e segredos ☁️
#
# **O que é** — O Databricks na Azure é um serviço de primeira parte da Microsoft: o **workspace** é um recurso
# Azure (`Microsoft.Databricks/workspaces`), com o **plano de controle** (UI, API, jobs) gerido pela Databricks e o
# **plano de computação** clássico rodando em VMs **na sua assinatura**, num *managed resource group*.
#
# **Por que importa** — Em empresa, as perguntas são de rede e segurança: o cluster tem IP público? Fala com o
# storage por rede privada? Onde ficam os segredos? Quem acessa o ADLS — uma pessoa ou uma identidade gerenciada?
#
# **Como funciona**
#
# ```mermaid
# flowchart TB
#     subgraph CP["Plano de controle (Databricks)"]
#         UI["UI / REST API / Jobs"]
#     end
#     subgraph SUB["Sua assinatura Azure"]
#         subgraph VNET["VNet própria (VNet injection)"]
#             H["subnet host"] --- C["subnet container"]
#         end
#         KV["Key Vault"]
#         AC["Access Connector<br/>(identidade gerenciada)"]
#         ADLS["ADLS Gen2<br/>(landing, bronze, silver, gold)"]
#     end
#     UI -- "Secure Cluster Connectivity<br/>(sem IP público nos nós)" --> VNET
#     VNET -- "Private Endpoint" --> ADLS
#     VNET -- "secret scope" --> KV
#     AC -- "Storage Blob Data Contributor" --> ADLS
# ```
#
# - **Tier:** o **Standard** foi aposentado (sem criação desde 01/04/2026; os restantes migrados para Premium em
#   01/10/2026). Premium é o que tem Unity Catalog, controle de acesso e auditoria.
# - **VNet injection:** o workspace usa uma VNet sua com **duas subnets** delegadas a
#   `Microsoft.Databricks/workspaces` (*host* e *container*) e um NSG. Com **Secure Cluster Connectivity**
#   (*No Public IP*), os nós não têm IP público; a saída para a internet passa por NAT Gateway/firewall seu.
#   **Private Link** fecha também o acesso à UI/API. Serverless roda na rede da Databricks e alcança o seu storage
#   por *Network Connectivity Configuration* (NCC) com private endpoints.
# - **Acesso ao storage:** com Unity Catalog, o caminho recomendado é o **Access Connector for Azure Databricks**
#   (identidade gerenciada) + *storage credential* + *external location* — nada de chave de conta de storage em
#   código ou em `spark.conf`. Terraform disso no notebook 14.
# - **Secret scope apoiado no Key Vault:** o scope é uma **interface somente leitura** para o Key Vault; o segredo
#   é criado e rotacionado no Azure. Exige o modelo de permissão **Vault access policy** (o Azure RBAC **não** é
#   suportado para esse tipo de scope) e quem cria precisa de Contributor/Owner no cofre
#   ([Secret management](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/), atualizada em
#   09/2026). Permissão é por scope inteiro — segredos com públicos diferentes pedem cofres diferentes.
#
# ```bash
# # ☁️ criação (UI: https://<workspace>#secrets/createScope — com S maiúsculo) ou CLI:
# # (exige autenticação por token do Microsoft Entra ID — token pessoal (PAT) do Databricks não serve)
# databricks secrets create-scope --json '{
#   "scope": "kv-oss",
#   "scope_backend_type": "AZURE_KEYVAULT",
#   "backend_azure_keyvault": {
#     "resource_id": "/subscriptions/<sub>/resourceGroups/rg-oss/providers/Microsoft.KeyVault/vaults/kv-oss",
#     "dns_name": "https://kv-oss.vault.azure.net/"
#   }
# }'
# databricks secrets put-acl kv-oss data-engineers READ
# ```
#
# > 🎤 **Resposta de 30 s:** "Em Azure eu subo o workspace Premium com VNet injection e Secure Cluster
# > Connectivity, storage por private endpoint, acesso ao ADLS via Access Connector no Unity Catalog — sem chave
# > de storage — e segredos de terceiros num Key Vault exposto como secret scope somente leitura. Tudo em
# > Terraform."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Dimensionar as subnets:** cada nó usa um IP em cada subnet; /26 dá ~59 nós — subnet pequena é limite de
#   escala que só aparece em produção.
# - **Mounts (`dbutils.fs.mount`) e credential passthrough** são legados; com Unity Catalog, acesso é por external
#   location + grant.
# - **Segredos no Unity Catalog:** a Databricks lançou segredos como objetos do UC (namespace de 3 níveis, grants
#   do UC); o Key Vault-backed scope continua sendo o padrão quando o Key Vault é a fonte da verdade da empresa.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - VNet injection + Private Link aumentam a segurança e a complexidade (DNS privado, rotas, firewall). Para POC,
#   workspace gerenciado padrão é suficiente.
# - Databricks-backed scope é mais simples (sem Key Vault) mas espalha segredos fora do cofre corporativo.

# %% [markdown]
# ## 9. Paridade local × Databricks — tabela honesta 🧪
#
# | Recurso | Local (este repo) | Databricks | Igual? |
# |---|---|---|---|
# | API DataFrame / Spark SQL | Spark 4.2 | DBR 17.3 LTS (4.0) … 19 (4.2) | ✅ mesma API (atenção a features novas) |
# | Delta: MERGE, time travel, CDF, VACUUM | `delta-spark` 4.4 | Delta do DBR | ✅ semântica igual; versão diferente |
# | Ingestão incremental | file source + `availableNow` | **Auto Loader** (`cloudFiles`) | ⚠️ mesma ideia; Auto Loader tem schema evolution, rescued data, notificação de arquivo |
# | Catálogo | caminho (`delta.load(path)`) | **Unity Catalog** (`catalogo.schema.tabela`) | ❌ grants, lineage, row filter só no UC |
# | Segredos | variável de ambiente | `dbutils.secrets` + Key Vault | ⚠️ mesma intenção, outro mecanismo |
# | Orquestração | `Makefile` | Lakeflow Jobs (bundle) | ⚠️ mesma ordem de tarefas; sem agenda/retry local |
# | Pipelines declarativos | — | Lakeflow Spark Declarative Pipelines | ⚠️ `pyspark.pipelines` existe no Spark 4.1+ OSS; expectations e UI só no Databricks |
# | Photon, Liquid automático, Predictive Optimization | — | ✅ | ❌ |
# | Storage | disco local; Azurite (notebook 14) | ADLS Gen2 `abfss://` | ⚠️ semântica de rename e consistência diferente |
# | Testes | `pytest` + Spark local | idem (no CI) + job de integração no alvo `dev` | ✅ |
# | System tables, Spark UI de serverless | — | ✅ | ❌ |
#
# O que **não** dá para afirmar a partir do laptop: tempo e custo de produção, comportamento com dezenas de nós,
# e limites de rede. Para isso existe o alvo `dev` do bundle.

# %%
print(f"Python {sys.version.split()[0]} local | serverless env 6 do Databricks usa Python 3.12.3 (mesma linha 3.12)")
print(f"Spark local {spark.version} | Delta {md.version('delta-spark')} | Java {sc._jvm.java.lang.System.getProperty('java.version')}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Eu desenvolvo e testo localmente porque é rápido e grátis, mas não finjo paridade:
# > API de DataFrame e semântica do Delta são as mesmas; Auto Loader, Unity Catalog, Photon e serverless não
# > existem no laptop. A lacuna fecha com o deploy do bundle num alvo `dev` e um teste de integração lá antes
# > de produção."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Armadilha de versão:** código que usa recurso do Spark 4.2 passa local e quebra num DBR 17.3 (Spark 4.0).
#   Regra: alinhar a versão local ao runtime de produção, ou testar no runtime de produção no CI.
# - **Python no serverless:** o *environment version* 6 (set/2026) usa Python 3.12.3 e Databricks Connect 19
#   ([serverless environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/)).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Se o time depende pesadamente de recursos só-Databricks (DLT/SDP com expectations, UC row filters), o ciclo
#   local cobre menos; Databricks Connect + alvo `dev` ganha peso.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```python
# # No notebook do Databricks, a sessão já existe — get_spark() só a devolve:
# from oss_lakehouse.spark import get_spark
# spark = get_spark()                     # DATABRICKS_RUNTIME_VERSION está definido → getOrCreate()
# spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")   # ex.: 17.3.x-scala2.13
#
# # Configs por cluster/job (não no código): na UI do compute ou no bundle (resources/jobs.yml)
# #   spark_env_vars: { OSSLH_ENV: databricks, OSSLH_DATA_ROOT: abfss://lake@<conta>.dfs.core.windows.net }
#
# # Listar runtimes disponíveis no workspace (chave exata para o bundle):
# #   databricks clusters spark-versions
# ```

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Qual a diferença entre driver e executor? Onde roda uma UDF Python?**
# <details><summary>Resposta</summary>
# Driver roda o programa, planeja e agenda; executors executam tasks sobre partições. A UDF roda nos executors,
# num processo Python ao lado da JVM, com serialização (Arrow, no caso de pandas UDF) — por isso é cara.
# </details>
#
# **2. O que significa `local[4]`? E `local[*]`?**
# <details><summary>Resposta</summary>
# Modo local: driver e executor na mesma JVM com 4 threads (ou todos os núcleos com `*`). Útil para desenvolvimento
# e testes; não representa custo de rede de um cluster.
# </details>
#
# **3. Aumentei `spark.driver.memory` no notebook e o OOM continua. Por quê?**
# <details><summary>Resposta</summary>
# É config estática da JVM: só vale na criação do processo. Precisa ir na config do cluster/`spark-submit`/builder
# antes de a sessão subir. `spark.conf.set` recusa com erro; o builder com `getOrCreate()` aceita calado e
# `spark.conf.get` passa a devolver o valor novo — mas o heap real não muda (prova na §2).
# </details>
#
# **4. Por que a versão do Delta precisa casar com a do Spark?**
# <details><summary>Resposta</summary>
# O JAR do Delta usa APIs internas do Spark; é publicado por versão (`delta-spark_4.2_2.13`). Descasar dá
# `NoSuchMethodError`/`ClassNotFoundException` em tempo de execução. No Databricks, o runtime já traz o Delta certo.
# </details>
#
# **5. All-purpose, job compute, serverless e SQL warehouse: quando usar cada um?**
# <details><summary>Resposta</summary>
# All-purpose para trabalho interativo; job compute (ou serverless) para jobs agendados — DBU mais barata e vida
# curta; serverless quando o início rápido e a gestão zero valem a perda de controle; SQL warehouse para SQL/BI.
# </details>
#
# **6. Quais os limites da Databricks Free Edition?**
# <details><summary>Resposta</summary>
# Só serverless (com cota diária), 1 SQL warehouse 2X-Small, 5 tasks de job simultâneas, 1 pipeline ativo por tipo,
# 1 workspace, sem SSO/rede privada, sem R/Scala, uso não comercial (doc de 29/09/2026).
# </details>
#
# **7. O que é Spark Connect e o que deixa de funcionar com ele?**
# <details><summary>Resposta</summary>
# Cliente-servidor por gRPC: o cliente envia o plano lógico. É a base do Databricks Connect e do serverless.
# Não há `sparkContext`, RDD nem acesso à JVM.
# </details>
#
# **8. Como você guarda o token de uma API usada pelo pipeline no Azure Databricks?**
# <details><summary>Resposta</summary>
# No Key Vault, exposto como secret scope (somente leitura, modelo de *access policy*), lido com
# `dbutils.secrets.get` ou referenciado na config do cluster como `{{secrets/scope/key}}`. ACL do scope para o
# service principal do job. Nunca em código, widget ou variável em texto.
# </details>
#
# **9. O que é VNet injection e por que uma empresa exige?**
# <details><summary>Resposta</summary>
# O plano de computação usa uma VNet do cliente (subnets host e container), permitindo NSG, rota pelo firewall
# corporativo, private endpoints para o storage e nós sem IP público (Secure Cluster Connectivity).
# </details>
#
# **10. O que funciona igual local e no Databricks, e o que não?**
# <details><summary>Resposta</summary>
# Igual: API DataFrame/SQL, semântica do Delta, testes. Diferente: Auto Loader, Unity Catalog, Photon, serverless,
# `dbutils`, system tables. A lacuna fecha com deploy em `dev` e teste de integração antes de prod.
# </details>
#
# **11. Volumes × DBFS mounts?**
# <details><summary>Resposta</summary>
# Volumes são objetos do Unity Catalog para arquivos, com grants e lineage; mounts são legado sem governança
# (qualquer um no workspace via o mount). Arquivo novo vai para Volume.
# </details>

# %% [markdown]
# ## Resumo
#
# - Driver planeja, executors executam, cluster manager aloca; `local[N]` = uma JVM com N threads.
# - Memória é config **estática**: `getOrCreate()` não muda o heap de uma sessão existente.
# - Delta local = JAR que casa com Spark e Scala (`delta-spark_4.2_2.13:4.4.0`); no Databricks vem no runtime.
# - Produção: job compute ou serverless, runtime LTS, wheel por bundle; Free Edition = só serverless e não comercial.
# - Azure: Premium, VNet injection + SCC, Access Connector para o ADLS, Key Vault-backed scope (access policy).

# %%
spark.stop()
