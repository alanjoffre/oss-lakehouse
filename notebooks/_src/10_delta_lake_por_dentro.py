# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 10 · Delta Lake por dentro
#
# > Abre o `_delta_log` e prova, arquivo por arquivo, como uma pasta de parquet vira uma tabela
# > com transação ACID, time travel, evolução de schema, deletion vectors e Change Data Feed —
# > e o que cada operação (DELETE, MERGE, OPTIMIZE, VACUUM) realmente grava.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Databricks e processamento de dados | §1–§12: o formato de tabela por trás de tudo no Databricks |
# | Arquitetura e desenvolvimento de pipelines | §3 (concorrência entre jobs), §5 (retenção), §10 (consumidor incremental com CDF) |
# | Python avançado | `oss_lakehouse.delta_log` (retry com backoff, dataclasses, consumidor idempotente), threads em §3 |
# | Microsoft Azure | ☁️ commit atômico no ADLS Gen2, tabelas gerenciadas pelo Unity Catalog, UniForm |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Versões.** Delta Lake OSS 4.4 sobre Spark 4.2. Vários recursos nasceram no Databricks e
# chegaram depois ao OSS; cada seção diz o que foi **testado aqui** e o que só existe na
# plataforma. Dados: amostra real da bronze (`gh_events`, GH Archive de 2026-10-01).

# %% [markdown]
# ## Setup

# %%
import json
import os
import shutil
import threading
import time
from importlib.metadata import version

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from oss_lakehouse.config import get_settings
from oss_lakehouse.delta_log import (
    apply_changes,
    log_dir,
    log_summary,
    physical_files,
    read_changes,
    read_commit,
    retry_on_conflict,
)
from oss_lakehouse.perf import as_table, delta_active_files, grep_plan, spark_conf, stopwatch
from oss_lakehouse.spark import get_spark

spark = get_spark("10")
s = get_settings()
D = s.path("demo", "10")  # tudo deste notebook (destrutivo) fica em data/demo/10
shutil.rmtree(D, ignore_errors=True)  # começa do zero: as versões citadas no texto são sempre as mesmas
os.makedirs(D)
print("Spark", spark.version, "| Delta", version("delta-spark"))


def one_line(exc: BaseException, n: int = 260) -> str:
    """Primeiros `n` caracteres da mensagem de erro, numa linha só."""
    text = " ".join(str(exc).split())
    start = text.find("[DELTA_")  # erro vindo da JVM: pula o preâmbulo do Py4J
    text = text[start:] if start > 0 else text
    return f"{type(exc).__name__}: {text[:n]}{'…' if len(text) > n else ''}"


def attempt(label: str, fn) -> None:
    """Executa `fn` e diz se passou ou qual erro deu (as seções de regra PRECISAM mostrar o erro)."""
    try:
        fn()
        print(f"✅ {label}: aceito")
    except Exception as exc:  # noqa: BLE001 — queremos mostrar a mensagem do Delta
        print(f"❌ {label}: {one_line(exc)}")


def short(obj, n: int = 330) -> str:
    text = json.dumps(obj, ensure_ascii=False)
    return text[:n] + ("…" if len(text) > n else "")


def show_log(path: str, last: int | None = None) -> None:
    print(as_table([c.as_row() for c in log_summary(path, last)]))


def detail(path: str) -> dict:
    r = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    return {"arquivos ativos": r.numFiles, "KB": round(r.sizeInBytes / 1024, 1)}


def protocol(path: str) -> dict:
    r = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    return {"leitor": r.minReaderVersion, "escritor": r.minWriterVersion,
            "table features": ", ".join(sorted(r.tableFeatures)) or "—"}


# %% [markdown]
# **Amostra.** 1 em cada 10 eventos da bronze (escolha determinística por `crc32(id)`), achatada
# em 6 colunas. São 3 horas de dados (12h, 13h e 14h UTC) — `event_hour` será a coluna de partição.

# %%
bronze = spark.read.format("delta").load(s.path("bronze", "gh_events"))
sample = (
    bronze.filter(F.crc32("id") % 10 == 0)
    .select("id", "type", F.col("actor.login").alias("actor_login"), F.col("repo.name").alias("repo_name"),
            F.to_timestamp("created_at").alias("created_at"))
    .withColumn("event_hour", F.hour("created_at"))
    .cache()
)
print(f"amostra: {sample.count():,} eventos")
sample.groupBy("event_hour").count().orderBy("event_hour").show()

# %% [markdown]
# ## 1. O transaction log (`_delta_log`) 🧪
#
# **O que é.** Uma tabela Delta é uma pasta com arquivos **parquet** (o dado) e uma subpasta
# `_delta_log/` com um arquivo JSON por **commit** (transação confirmada), numerado em ordem:
# `00000000000000000000.json`, `...001.json`… Cada linha do JSON é uma **ação**. O estado da
# tabela numa versão N é o resultado de aplicar, em ordem, as ações dos commits 0..N — o
# *log replay*.
#
# **Por que importa.** É o log, e não a listagem da pasta, que diz quais arquivos fazem parte da
# tabela. Disso sai tudo: atomicidade (o commit existe ou não existe), isolamento (cada leitor
# fixa uma versão), time travel (ler a versão N), auditoria (`commitInfo`) e data skipping
# (estatísticas por arquivo). Um parquet solto na pasta, sem `add` no log, **não existe** para
# a tabela.
#
# **Como funciona.** As ações:
#
# | Ação | O que registra |
# |---|---|
# | `commitInfo` | quem/quando/qual operação e métricas — auditoria, não afeta o estado |
# | `protocol` | versão mínima de leitor/escritor e *table features* exigidas (§11) |
# | `metaData` | schema, colunas de partição, propriedades da tabela |
# | `add` | um arquivo de dados passa a valer: caminho, partição, tamanho, **estatísticas** |
# | `remove` | um arquivo deixa de valer (*tombstone*) — o arquivo **continua no disco** |
# | `cdc`, `txn`, `domainMetadata` | Change Data Feed (§10), idempotência de streaming, metadados de features |
#
# Criamos a tabela com as horas 12 e 13 (commit 0), fazemos um append da hora 14 (commit 1) e
# um `DELETE` (commit 2).

# %%
EV = f"{D}/events"
(sample.filter("event_hour < 14").repartition("event_hour")
 .write.format("delta").partitionBy("event_hour").save(EV))                              # commit 0
(sample.filter("event_hour = 14").coalesce(1)
 .write.format("delta").mode("append").save(EV))                                         # commit 1
spark.sql(f"DELETE FROM delta.`{EV}` WHERE type = 'WatchEvent'")                         # commit 2

for p in sorted(log_dir(EV).iterdir()):
    if p.is_file() and not p.name.startswith("."):  # o Hadoop local cria um .crc oculto por arquivo: ruído
        print(f"{p.name:<32} {p.stat().st_size:>7,} bytes")

# %% [markdown]
# Um `.json` por commit. Os `.crc` ao lado são o *checksum* de versão do Delta: um resumo do
# estado (nº de arquivos, bytes, protocolo, metadados) que permite validar o log replay sem
# refazê-lo. Agora o conteúdo do **commit 0**, ação por ação (texto cortado):

# %%
for action in read_commit(EV, 0):
    (kind, body), = action.items()
    if kind == "add":  # `stats` vem como string JSON dentro do JSON: abrimos para ler
        st = json.loads(body["stats"])
        body = {"path": body["path"], "partitionValues": body["partitionValues"], "size": body["size"],
                "dataChange": body["dataChange"],
                "stats": {"numRecords": st["numRecords"],
                          "minValues": {"created_at": st["minValues"]["created_at"]},
                          "maxValues": {"created_at": st["maxValues"]["created_at"]},
                          "nullCount": {"repo_name": st["nullCount"]["repo_name"]}}}
    print(f"{kind:<10} {short(body)}\n")

# %% [markdown]
# Leitura: `commitInfo` diz que foi um `WRITE` com `partitionBy` e `isBlindAppend: true`
# (escreveu sem ler a tabela — isso importa para conflito, §3); `metaData` carrega o schema
# (como string JSON) e `partitionColumns`; `protocol` diz o que um cliente precisa saber fazer
# para ler/escrever; e cada `add` traz o valor da partição **no log** (`partitionValues` — o
# leitor não precisa interpretar o nome da pasta) e as estatísticas `numRecords`, `minValues`,
# `maxValues`, `nullCount`, que são a base do data skipping (notebook 09 §7).
#
# Os três commits, resumidos pelo `oss_lakehouse.delta_log.log_summary` (contado direto do JSON):

# %%
show_log(EV)
active = delta_active_files(EV)  # log replay feito à mão: add liga, remove desliga
n_rows = spark.read.format("delta").load(EV).count()
print(f"\nlog replay à mão : {len(active)} arquivos ativos, {sum(f['num_records'] for f in active):,} linhas")
print(f"o que o Delta diz: {detail(EV)['arquivos ativos']} arquivos ativos, {n_rows:,} linhas")
print(f"no disco         : {physical_files(EV)['parquet']} arquivos parquet")

# %% [markdown]
# O `DELETE` não alterou nenhum parquet (parquet é imutável): **reescreveu** cada arquivo que
# tinha `WatchEvent` sem essas linhas (`add`) e marcou o original como removido (`remove`). É o
# *copy-on-write*. Por isso o disco tem mais parquet do que a tabela tem arquivos ativos: os
# removidos ficam lá, servindo o time travel (§4), até o `VACUUM` (§5). E o log replay feito à
# mão, só com os JSON, chega ao mesmo número de arquivos e de linhas que o Delta.
#
# > 🎤 **Resposta de 30 s:** "Delta é parquet mais um log de transações. Cada commit é um JSON
# > numerado em `_delta_log` com ações: `add` e `remove` de arquivos, `metaData` com o schema,
# > `protocol` e `commitInfo`. O estado da tabela é o replay dessas ações. Como o commit é a
# > criação atômica de um arquivo, a escrita é tudo-ou-nada; como o leitor fixa uma versão, ele
# > nunca vê escrita pela metade; e como `remove` não apaga o arquivo, dá para ler versões
# > antigas."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **O que torna o commit atômico?** Criar o arquivo `N.json` com semântica *put-if-absent*
#   (falha se já existe). No ADLS Gen2 isso é o rename atômico do namespace hierárquico; no S3
#   foi por anos o ponto fraco (exigia um coordenador externo). No Databricks com Unity Catalog,
#   o commit de tabela gerenciada passa pelo catálogo (*catalog-managed commits*) ☁️.
# - **Arquivo órfão**: se o job morre depois de gravar parquet e antes do commit, os arquivos
#   ficam na pasta sem `add`. Ninguém os lê; o `VACUUM` limpa.
# - **`dataChange`**: `false` em `add`/`remove` significa "reorganizei, o conteúdo é o mesmo"
#   (OPTIMIZE, §12). Leitores de streaming ignoram esses commits.
# - **Estatísticas**: só das primeiras 32 colunas por padrão (`delta.dataSkippingNumIndexedCols`)
#   ou das listadas em `delta.dataSkippingStatsColumns`. String longa é truncada no min/max.
# - **Idempotência de streaming**: a ação `txn` (appId + versão) é o que impede um micro-lote
#   reprocessado de gravar duas vezes (notebook 06).
# </details>
#
# **Trade-offs / quando NÃO usar:** copy-on-write torna barata a leitura e cara a alteração
# pontual (apagar 1 linha reescreve o arquivo inteiro — é o que os deletion vectors atacam, §9);
# e o log cresce a cada commit: muita escrita pequena (streaming com trigger curto) = log
# longo, daí o checkpoint (§2).

# %% [markdown]
# ## 2. Checkpoint: o log não é relido do zero 🧪
#
# **O que é.** A cada N commits (padrão `delta.checkpointInterval` = 10) o Delta grava um
# **checkpoint**: um parquet com o estado **já reconciliado** da tabela naquela versão — os
# `add` ativos, os `remove` ainda dentro da retenção, o `metaData` e o `protocol` vigentes.
#
# **Por que importa.** Sem checkpoint, abrir uma tabela com 100 mil commits exigiria ler 100
# mil JSON. Com ele, o leitor lê `_last_checkpoint` (um ponteiro), carrega o checkpoint e
# aplica só os JSON posteriores.
#
# **Como funciona.** Para não fazer 10 commits, baixamos o intervalo desta tabela para 2 e
# fazemos mais um commit.

# %%
spark.sql(f"ALTER TABLE delta.`{EV}` SET TBLPROPERTIES ('delta.checkpointInterval' = '2')")  # commit 3
spark.sql(f"UPDATE delta.`{EV}` SET repo_name = lower(repo_name) "
          "WHERE event_hour = 14 AND type = 'ForkEvent'")                                    # commit 4
names = sorted(p.name for p in log_dir(EV).iterdir()
               if p.is_file() and not p.name.startswith(".") and not p.name.endswith(".crc"))
print("\n".join(names))
print("\n_last_checkpoint:", (log_dir(EV) / "_last_checkpoint").read_text()[:230])

# %%
cp_file = next(p for p in log_dir(EV).iterdir() if p.name.endswith(".checkpoint.parquet"))
cp = spark.read.parquet(str(cp_file))
print("colunas do checkpoint (uma por tipo de ação):", cp.columns)
cp.select([F.count(c).alias(c) for c in cp.columns]).show()
print("campos do add:", cp.select("add.*").columns)
print("arquivos ativos segundo o Delta:", detail(EV)["arquivos ativos"])

# %% [markdown]
# O checkpoint é uma tabela "larga": uma coluna (struct) por tipo de ação, uma linha por ação.
# O número de linhas com `add` preenchido é exatamente o número de arquivos ativos; as linhas
# `remove` são os *tombstones* ainda guardados (o `VACUUM` precisa deles para saber o que pode
# apagar e desde quando). `commitInfo` **não** entra: é histórico, não estado.
#
# > 🎤 **Resposta de 30 s:** "A cada 10 commits o Delta grava um checkpoint em parquet com o
# > estado consolidado — arquivos ativos, schema, protocolo. Para abrir a tabela, o leitor vai
# > ao `_last_checkpoint`, carrega esse parquet e aplica só os JSON mais novos. É o que mantém
# > o custo de abrir a tabela independente do tamanho do histórico."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Limpeza do log**: JSON e checkpoints mais velhos que `delta.logRetentionDuration`
#   (padrão 30 dias) são apagados quando um checkpoint novo é gravado. Time travel além disso é
#   impossível mesmo que os parquet existam.
# - **Estatísticas no checkpoint**: nos "campos do add" acima aparecem as duas formas —
#   `stats` (JSON em string) e `stats_parsed` (struct tipada: o leitor filtra por min/max sem
#   parsear JSON), além de `partitionValues_parsed`. Controlado por
#   `delta.checkpoint.writeStatsAsJson` / `writeStatsAsStruct`.
# - **Checkpoint multi-part e V2**: tabela com milhões de arquivos divide o checkpoint em vários
#   parquet; o *V2 checkpoint* (`delta.checkpointPolicy = v2`) separa em *sidecars* reaproveitáveis.
# - **Tabela com milhões de arquivos ativos** = checkpoint de GBs = driver lento para abrir a
#   tabela. É mais um custo de small files (notebook 09 §6).
# </details>
#
# **Trade-offs:** intervalo curto de checkpoint = leitura mais rápida do log e mais escrita a
# cada commit. No Databricks o intervalo é ajustado pela plataforma; mexer nisso é raro.

# %% [markdown]
# ## 3. ACID e controle de concorrência otimista 🧪
#
# **O que é.** ACID no Delta:
#
# - **Atomicidade** — o commit é um arquivo: ou existe inteiro, ou não existe (§1; prova com
#   lote rejeitado em §7).
# - **Consistência** — schema enforcement e constraints barram dado inválido (§6, §7).
# - **Isolamento** — leitores usam *snapshot isolation* (fixam uma versão e não veem commits
#   posteriores); escritores usam **controle de concorrência otimista** (OCC).
# - **Durabilidade** — commit confirmado = arquivo no storage (ADLS replica).
#
# **Por que importa.** Num lakehouse real há vários jobs na mesma tabela: ingestão, MERGE de
# correção, DELETE de LGPD, OPTIMIZE. Não há *lock*: cada um trabalha achando que não haverá
# conflito e confere no fim. Quando há, **um deles falha** — e é preciso saber por quê e como
# desenhar para não acontecer.
#
# **Como funciona.** Toda escrita segue três passos:
#
# ```text
# 1. LER      fixa a versão N e anota o que leu (quais arquivos / qual predicado)
# 2. ESCREVER grava parquet novos na pasta (ninguém vê ainda)
# 3. VALIDAR  tenta criar (N+1).json
#             ├─ não existe → commit feito
#             └─ já existe (alguém commitou antes) → lê o que o vencedor fez:
#                   ├─ não toca no que eu li → tenta (N+2).json (repetição automática, invisível)
#                   └─ toca no que eu li     → ConcurrentAppendException / ConcurrentDeleteReadException / …
# ```
#
# O ponto fino é o "toca no que eu li". O Delta compara os arquivos que o vencedor
# adicionou/removeu com o **predicado de leitura** do perdedor. Se o perdedor leu "a tabela
# inteira", qualquer arquivo novo conflita.
#
# **O experimento (conflito real, duas threads).** Dois jobs na tabela particionada por
# `event_hour`:
#
# - **Job A** — um MERGE lento que normaliza `repo_name` de 200 eventos **da hora 12**. A
#   origem passa por uma UDF que dorme alguns segundos (simula um job demorado).
# - **Job B** — um `DELETE` dos eventos de um ator **na hora 13**, disparado enquanto A trabalha.
#
# Os dois mexem em **partições diferentes**. Rodamos duas vezes; a única diferença é a condição
# do MERGE de A.

# %%
SLOW_SECONDS = 6
slow_id = sample.filter("event_hour = 12").orderBy("id").first()["id"]
victim = (sample.filter("event_hour = 13").groupBy("actor_login").count()
          .orderBy(F.desc("count"), "actor_login").first()["actor_login"])


@F.udf("string")
def slow_lower(repo_name, event_id):
    if event_id == slow_id:  # uma única linha "demora": o job A leva pelo menos SLOW_SECONDS
        time.sleep(SLOW_SECONDS)
    return repo_name.lower() if repo_name else None


def updates():
    return (sample.filter("event_hour = 12").orderBy("id").limit(200)
            .select("id", slow_lower("repo_name", "id").alias("repo_name")))


def fresh_table(name: str) -> str:
    path = f"{D}/{name}"
    shutil.rmtree(path, ignore_errors=True)
    sample.repartition("event_hour").write.format("delta").partitionBy("event_hour").save(path)
    return path


def job_a(path: str, condition: str) -> None:
    (DeltaTable.forPath(spark, path).alias("t").merge(updates().alias("s"), condition)
     .whenMatchedUpdate(set={"repo_name": "s.repo_name"}).execute())


def job_b(path: str) -> None:
    spark.sql(f"DELETE FROM delta.`{path}` WHERE event_hour = 13 AND actor_login = '{victim}'")


def race(path: str, run_a) -> None:
    """Dispara A numa thread, espera A abrir a transação e roda B por cima. Mostra quem commitou."""
    outcome: dict[str, str] = {}
    t0 = time.perf_counter()

    def target() -> None:
        try:
            run_a()
            outcome["A"] = f"commit OK aos {time.perf_counter() - t0:.1f}s"
        except Exception as exc:  # noqa: BLE001
            outcome["A"] = f"FALHOU aos {time.perf_counter() - t0:.1f}s → {one_line(exc, 330)}"

    thread = threading.Thread(target=target)
    thread.start()
    time.sleep(SLOW_SECONDS / 2)  # A já leu o snapshot (versão 0) e está "trabalhando"
    job_b(path)
    outcome["B"] = f"commit OK aos {time.perf_counter() - t0:.1f}s"
    thread.join()
    print(f"job B (DELETE hora 13): {outcome['B']}")
    print(f"job A (MERGE hora 12) : {outcome['A']}")
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "operation").orderBy("version")
    print("histórico:", [(r.version, r.operation) for r in hist.collect()])


print("vítima do DELETE:", victim, "\n")
print("── Rodada 1: MERGE com condição só pela chave (t.id = s.id)")
OCC1 = fresh_table("occ_sem_particao")
race(OCC1, lambda: job_a(OCC1, "t.id = s.id"))

# %% [markdown]
# O job A **falhou** com `ConcurrentAppendException` — mesmo sem tocar em nenhuma linha da hora
# 13. A própria mensagem aponta a causa (a operação concorrente mexeu na partição
# `event_hour=13`, "que deveria ter sido lida" por A) e sugere o conserto. Motivo: a condição
# `t.id = s.id` não diz em que partição procurar, então A "leu a tabela inteira". Quando B
# commitou um arquivo novo na hora 13 (o reescrito pelo DELETE), o Delta não tem como garantir
# que A teria tomado a mesma decisão vendo esse arquivo, e aborta A. Nenhum dado de A entrou:
# o histórico só tem o WRITE e o DELETE.
#
# **Conserto 1 — dizer a partição na condição.** Mesmos jobs; A agora declara que só lê a
# hora 12 (`AND t.event_hour = 12`).

# %%
print("── Rodada 2: MERGE com a partição na condição (t.id = s.id AND t.event_hour = 12)")
OCC2 = fresh_table("occ_com_particao")
race(OCC2, lambda: job_a(OCC2, "t.id = s.id AND t.event_hour = 12"))

# %% [markdown]
# Os dois commitaram. A perdeu a corrida pelo arquivo `1.json`, viu que o commit de B só mexeu
# na hora 13 — fora do que A declarou ler —, e commitou sozinho na versão seguinte. Essa
# repetição é automática e invisível.
#
# **Conserto 2 — tentar de novo.** Quando os jobs realmente disputam os mesmos dados, o
# conflito é legítimo e a saída é repetir a operação **inteira** (relendo o snapshot novo).
# `oss_lakehouse.delta_log.retry_on_conflict` faz isso com backoff exponencial, só para as
# exceções de conflito de dados. Rodada 1 de novo, agora com retry:

# %%
print("── Rodada 3: a condição ruim da rodada 1, com retry_on_conflict")
OCC3 = fresh_table("occ_com_retry")
race(OCC3, lambda: retry_on_conflict(
    lambda: job_a(OCC3, "t.id = s.id"), attempts=3, base_delay=0.2,
    on_retry=lambda n, exc: print(f"   tentativa {n} do job A perdeu ({type(exc).__name__}); repetindo…")))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Delta não usa lock: é concorrência otimista. Cada escrita lê uma
# > versão, grava arquivos e tenta criar o próximo JSON do log; se outro commit chegou antes, o
# > Delta confere se ele mexeu em algo que eu li. Se não, recommita sozinho; se sim, lança
# > `ConcurrentAppendException` ou parente. Eu evito no desenho — jobs concorrentes em partições
# > disjuntas e com a partição explícita na condição do MERGE/UPDATE/DELETE — e trato o resto
# > com retry, porque essas operações são idempotentes."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **As exceções** (`delta.exceptions`): `ConcurrentAppendException` (o outro adicionou arquivo
#   onde eu li), `ConcurrentDeleteReadException` (o outro removeu arquivo que eu li),
#   `ConcurrentDeleteDeleteException` (os dois removeram o mesmo arquivo — ex.: dois OPTIMIZE),
#   `MetadataChangedException` (schema/propriedade mudou), `ProtocolChangedException`,
#   `ConcurrentTransactionException` (dois streams com o mesmo checkpoint).
# - **Níveis de isolamento de escrita**: `WriteSerializable` (padrão) e `Serializable`
#   (`delta.isolationLevel`). No padrão, **append cego** (INSERT que não lê a tabela) nunca
#   conflita com nada — por isso dois jobs de ingestão em append convivem sem cuidado nenhum.
# - **Row-level concurrency** ☁️ (Databricks, tabelas com deletion vectors): o conflito passa a
#   ser por **linha**, não por arquivo — dois UPDATE em linhas diferentes do mesmo arquivo
#   passam. Liquid Clustering também o habilita. No OSS a detecção é por arquivo/partição.
# - **Streaming + batch na mesma tabela**: o stream em append não conflita; o MERGE/DELETE em
#   batch pode conflitar com OPTIMIZE — agende ou particione.
# - **Não há transação multi-tabela** no Delta OSS: cada tabela tem o seu log. (O Databricks
#   vem introduzindo transações multi-instrução/multi-tabela coordenadas pelo catálogo ☁️ —
#   conferir a disponibilidade no runtime em uso antes de prometer.)
# </details>
#
# **Trade-offs / quando NÃO usar:** OCC é ótimo quando conflito é raro (o caso de pipelines de
# dados). Muitos escritores disputando as mesmas linhas — carga transacional — desperdiçam
# trabalho em retry: isso é serviço para um banco OLTP (no Databricks, Lakebase), não para Delta.

# %% [markdown]
# ## 4. Histórico, time travel e RESTORE 🧪
#
# **O que é.** Como `remove` não apaga arquivo e o log guarda cada versão, dá para **ler a
# tabela como ela era** (*time travel*) por número de versão ou por instante, e para **voltar**
# a tabela a uma versão antiga (`RESTORE`).
#
# **Por que importa.** Auditoria ("que dado o relatório de ontem leu?"), reprodutibilidade
# (treinar modelo na mesma versão), depuração (comparar antes/depois de um job) e recuperação
# de desastre operacional (um `DELETE` sem `WHERE`).
#
# **Como funciona.** `DESCRIBE HISTORY` lê os `commitInfo` do log; `DESCRIBE DETAIL` mostra o
# estado atual (arquivos, tamanho, partição, protocolo).

# %%
hist = (spark.sql(f"DESCRIBE HISTORY delta.`{EV}`")
        .select("version", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss.SSS").alias("timestamp_utc"),
                "operation", F.col("operationMetrics").cast("string").alias("operationMetrics"))
        .orderBy("version"))
hist.show(truncate=62)
d = spark.sql(f"DESCRIBE DETAIL delta.`{EV}`").first()
print({k: d[k] for k in ("format", "numFiles", "sizeInBytes", "partitionColumns", "minReaderVersion",
                         "minWriterVersion", "properties")})

# %%
for v in range(5):
    n = spark.read.format("delta").option("versionAsOf", v).load(EV).count()
    print(f"versionAsOf {v}: {n:>7,} linhas")
ts1 = hist.filter("version = 1").first()["timestamp_utc"]
n_ts = spark.read.format("delta").option("timestampAsOf", ts1).load(EV).count()
n_sql = spark.sql(f"SELECT count(*) FROM delta.`{EV}` VERSION AS OF 1").first()[0]
print(f"\ntimestampAsOf '{ts1}' (o instante do commit 1): {n_ts:,} linhas | SQL VERSION AS OF 1: {n_sql:,}")

# %% [markdown]
# As versões 0 e 1 mostram a tabela antes e depois do append; a 2 em diante, depois do
# `DELETE`. `timestampAsOf` resolve para **a última versão commitada até aquele instante**.
#
# **RESTORE.** Suponha que o `DELETE` do commit 2 foi um erro. `RESTORE ... TO VERSION AS OF 1`
# não apaga histórico: grava um commit **novo** cujo efeito é reativar os arquivos da versão 1
# (`add`) e desativar os que vieram depois (`remove`). Nenhum parquet é reescrito — na tabela
# abaixo, as colunas de KB somam o tamanho dos arquivos **re-referenciados** pelos `add` (o
# resumo do commit não sabe que eles já existiam); a prova é a contagem de parquet no disco,
# que não muda.

# %%
before = physical_files(EV)["parquet"]
r = spark.sql(f"RESTORE TABLE delta.`{EV}` TO VERSION AS OF 1").first()
print({k: r[k] for k in ("num_restored_files", "num_removed_files", "table_size_after_restore")})
show_log(EV, last=1)
print(f"\nlinhas agora: {spark.read.format('delta').load(EV).count():,} (iguais às da versão 1)")
print(f"parquet no disco: {before} antes → {physical_files(EV)['parquet']} depois do RESTORE (nada reescrito)")
print(f"a versão 4 continua legível: {spark.read.format('delta').option('versionAsOf', 4).load(EV).count():,} linhas")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Cada commit é uma versão. `versionAsOf` ou `timestampAsOf` lê a
# > tabela como era; `DESCRIBE HISTORY` mostra quem fez o quê; `RESTORE` volta a tabela gravando
# > um commit novo que reativa os arquivos antigos — é barato e reversível. O limite é a
# > retenção: depois do VACUUM os arquivos antigos somem e o time travel para aquela versão
# > falha. Time travel é para auditoria e recuperação de curto prazo, não é backup."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **O timestamp do commit** é, por padrão, a data de modificação do arquivo JSON no storage —
#   copiar a tabela para outro lugar muda os instantes. A feature *in-commit timestamps*
#   (`delta.enableInCommitTimestamps`) grava o instante dentro do commit.
# - **RESTORE conflita** com escritas concorrentes como qualquer commit, e falha se os arquivos
#   da versão-alvo já foram apagados pelo VACUUM.
# - **Streaming depois de RESTORE**: consumidores de CDF/stream veem o RESTORE como mudanças
#   (linhas reaparecendo). Avisar quem consome.
# - **CLONE**: `SHALLOW CLONE` copia só o log (aponta para os mesmos parquet — ambiente de
#   teste barato); `DEEP CLONE` copia dados também ☁️ (deep clone é do Databricks; o OSS tem shallow).
# - **Sintaxe curta**: `SELECT * FROM tabela VERSION AS OF 3`, `TIMESTAMP AS OF '2026-10-01'`,
#   `tabela@v3`.
# </details>
#
# **Trade-offs:** histórico longo = storage pago (os arquivos antigos continuam lá) e mais
# risco de LGPD: um dado "apagado" continua legível por time travel até o VACUUM.

# %% [markdown]
# ## 5. VACUUM e retenção 🧪
#
# **O que é.** `VACUUM` apaga **do storage** os arquivos que não fazem parte da versão atual
# **e** foram removidos do log há mais tempo que a retenção
# (`delta.deletedFileRetentionDuration`, padrão **7 dias**), além dos arquivos órfãos (sem
# `add`). É a única operação do Delta que destrói dado de verdade.
#
# **Por que importa.** Sem VACUUM o storage só cresce (cada UPDATE/DELETE/OPTIMIZE deixa a cópia
# antiga). Com VACUUM agressivo demais, perde-se time travel — e, pior, pode-se **corromper**
# leituras e escritas em andamento.
#
# **Como funciona.** Tabela de uma partição só: commit 0 grava, commit 1 faz um `DELETE` (que
# reescreve o arquivo). Há 2 parquet no disco e 1 ativo.

# %%
VT = f"{D}/vacuum"
sample.filter("event_hour = 12").coalesce(1).write.format("delta").save(VT)       # versão 0
spark.sql(f"DELETE FROM delta.`{VT}` WHERE type = 'PushEvent'")                   # versão 1
n_v0 = spark.read.format("delta").option("versionAsOf", 0).load(VT).count()
print(f"versão 0: {n_v0:,} linhas | versão 1: {spark.read.format('delta').load(VT).count():,} linhas")
print("no disco:", physical_files(VT), "| ativos:", detail(VT)["arquivos ativos"])
tipos_v0 = spark.read.format("delta").option("versionAsOf", 0).load(VT).groupBy("type").count().count()
print(f"versão 0, lendo os dados de verdade: {tipos_v0} tipos de evento distintos")

dry = spark.sql(f"VACUUM delta.`{VT}` DRY RUN")
print(f"\nVACUUM DRY RUN com a retenção padrão (7 dias): {dry.count()} arquivo(s) seriam apagados")
attempt("VACUUM RETAIN 0 HOURS (checagem ligada)", lambda: spark.sql(f"VACUUM delta.`{VT}` RETAIN 0 HOURS"))

# %% [markdown]
# Com a retenção padrão, nada é apagado: o arquivo antigo foi removido do log há segundos, não
# há 7 dias. E pedir retenção menor que a configurada é **barrado** pela checagem de segurança
# `spark.databricks.delta.retentionDurationCheck.enabled`.
#
# **Por que 7 dias e por que a trava.** O VACUUM decide pelo relógio, e há dois riscos reais:
#
# 1. **Leitor em andamento.** Um job que fixou a versão antiga há 2 horas ainda está lendo os
#    arquivos dela. Se o VACUUM os apaga, o job quebra com `FileNotFoundException` no meio.
# 2. **Escritor em andamento.** Um job gravou parquet e ainda não commitou. Para o VACUUM esses
#    arquivos são órfãos; com retenção 0 ele os apaga, e o commit que vem depois aponta para
#    arquivos que não existem → **tabela corrompida**.
#
# A retenção tem de ser maior que o job mais longo e que a defasagem do consumidor mais lento.
# 7 dias cobre isso com folga e dá uma semana de time travel para recuperação.
#
# **Só para o demo** — numa tabela que ninguém mais está lendo ou escrevendo — desligamos a
# checagem para ver o efeito:

# %%
with spark_conf(spark, {"spark.databricks.delta.retentionDurationCheck.enabled": "false"}):
    spark.sql(f"VACUUM delta.`{VT}` RETAIN 0 HOURS")
print("no disco depois do VACUUM:", physical_files(VT), "| ativos:", detail(VT)["arquivos ativos"])
print(f"versão atual: {spark.read.format('delta').load(VT).count():,} linhas (intacta)\n")
v0 = lambda: spark.read.format("delta").option("versionAsOf", 0).load(VT)  # noqa: E731
print("count(*) da versão 0 (respondido só com as estatísticas do log):", f"{v0().count():,}")
attempt("ler os DADOS da versão 0 (groupBy type) depois do VACUUM", lambda: v0().groupBy("type").count().collect())
print()
spark.sql(f"DESCRIBE HISTORY delta.`{VT}`").select("version", "operation", "operationParameters") \
    .orderBy("version").show(truncate=70)

# %% [markdown]
# O parquet da versão 0 foi apagado, e qualquer consulta que precise **abrir o arquivo** falha:
# time travel depende do log **e** dos arquivos de dados. A versão atual segue intacta.
#
# Dois detalhes que a saída mostra e que enganam em produção:
#
# - **`count(*)` da versão 0 ainda responde.** O Delta resolve `count(*)` sem filtro somando o
#   `numRecords` das estatísticas do log — não abre parquet nenhum. Ou seja: "testei o time
#   travel com um `count` e funcionou" **não prova** que os dados ainda existem.
# - **O log continua conhecendo a versão 0** (ela aparece no histórico). O VACUUM não mexe no
#   log; ele próprio fica registrado como dois commits, `VACUUM START` e `VACUUM END`.
#
# > 🎤 **Resposta de 30 s:** "VACUUM apaga do storage os arquivos fora da versão atual e mais
# > velhos que a retenção — 7 dias por padrão. Depois dele, time travel para versões anteriores
# > falha. Os 7 dias protegem leitores e escritores em andamento: com retenção zero dá para
# > apagar arquivo de uma transação que ainda não commitou e corromper a tabela. Eu nunca
# > desligo a checagem em produção; ajusto a retenção pela janela de recuperação que o negócio
# > precisa e pelo custo de storage — e para LGPD lembro que apagar de verdade é DELETE **mais**
# > VACUUM."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Duas retenções diferentes**: `delta.deletedFileRetentionDuration` (7 dias — arquivos de
#   dados, usado pelo VACUUM) e `delta.logRetentionDuration` (30 dias — JSON/checkpoints).
#   Time travel de 30 dias exige subir a primeira para 30 dias **e** pagar o storage.
# - **Custo do VACUUM**: listar a pasta inteira e comparar com o log. Em tabela enorme, a
#   listagem é o gargalo. `VACUUM ... LITE` (Delta 3.3+) usa só o log para achar o que apagar,
#   sem listar — mas não pega arquivo órfão; o `FULL` (padrão) pega.
# - **LGPD / direito ao esquecimento**: DELETE tira do estado atual; o dado segue nos parquet
#   antigos até o VACUUM. Com deletion vectors (§9) é preciso ainda `REORG ... APPLY (PURGE)`.
# - **Soft delete do ADLS** e *lifecycle policies* agem por baixo do Delta: um lifecycle que
#   apaga blobs antigos por data corrompe a tabela (apaga parquet ativo antigo). Nunca.
# - `VACUUM` também limpa os arquivos de `_change_data` (CDF) com a mesma retenção.
# </details>
#
# **Trade-offs:** retenção longa = mais time travel e mais custo/exposição de dado; curta = o
# contrário. No Databricks com Unity Catalog, a Predictive Optimization roda o VACUUM sozinha ☁️.

# %% [markdown]
# ## 6. Schema enforcement × schema evolution 🧪
#
# **O que é.** *Schema enforcement* (ou *schema on write*): o Delta recusa uma escrita cujo
# schema não bate com o da tabela. *Schema evolution*: aceitar a mudança **quando pedida
# explicitamente** (`mergeSchema`), gravando um `metaData` novo no log.
#
# **Por que importa.** Num data lake de parquet puro, um job com uma coluna a mais ou um tipo
# trocado grava sem erro e quebra todos os leitores depois. No Delta o erro aparece **na
# escrita**, no job que causou.

# %%
ST = f"{D}/schema"
base = sample.filter("event_hour = 12").limit(1000).select("id", "type", "actor_login", "event_hour")
base.write.format("delta").save(ST)
extra = sample.filter("event_hour = 13").limit(100).select("id", "type", "actor_login", "event_hour") \
    .withColumn("is_bot", F.col("actor_login").endswith("[bot]"))

attempt("append com coluna nova (is_bot)", lambda: extra.write.format("delta").mode("append").save(ST))
attempt("o mesmo append com mergeSchema",
        lambda: extra.write.format("delta").mode("append").option("mergeSchema", "true").save(ST))
attempt("append com tipo errado (event_hour como texto)",
        lambda: extra.withColumn("event_hour", F.lit("treze")).write.format("delta").mode("append").save(ST))
attempt("append SEM uma coluna (actor_login)",
        lambda: extra.drop("actor_login").write.format("delta").mode("append").save(ST))
print()
show_log(ST)
print("ações do commit 1 (o do mergeSchema):", log_summary(ST)[1].actions, "\n")
spark.read.format("delta").load(ST).groupBy(F.col("is_bot").isNull().alias("is_bot é NULL"),
                                            F.col("actor_login").isNull().alias("actor_login é NULL")).count().show()

# %% [markdown]
# Quatro comportamentos: coluna a mais → **recusado**; a mesma escrita com `mergeSchema` →
# aceito (o schema novo entra como `metaData` no mesmo commit do `add` — atômico, veja "ações
# do commit 1"; as linhas antigas leem `NULL`); tipo incompatível → **recusado mesmo com evolução** (string não cabe em
# inteiro); coluna a menos → aceito, a coluna ausente vira `NULL` (a menos que seja `NOT NULL`, §7).
#
# **Mudança de tipo.** O Delta 4 tem *type widening* (alargamento de tipo): com a propriedade
# ligada, `INT → BIGINT` vira mudança só de metadados (os dois commits abaixo têm zero `add` e
# zero `remove`; o leitor converte o inteiro antigo na leitura). Testando neste ambiente:

# %%
attempt("ALTER COLUMN event_hour TYPE BIGINT (sem type widening)",
        lambda: spark.sql(f"ALTER TABLE delta.`{ST}` ALTER COLUMN event_hour TYPE BIGINT"))
attempt("ligar delta.enableTypeWidening",
        lambda: spark.sql(f"ALTER TABLE delta.`{ST}` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"))
attempt("ALTER COLUMN event_hour TYPE BIGINT (com type widening)",
        lambda: spark.sql(f"ALTER TABLE delta.`{ST}` ALTER COLUMN event_hour TYPE BIGINT"))
print("tipo de event_hour agora:", dict(spark.read.format("delta").load(ST).dtypes)["event_hour"])
show_log(ST, last=2)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Por padrão o Delta faz schema enforcement: escrita com coluna a
# > mais ou tipo diferente falha. Evolução é opt-in — `mergeSchema` no append, `WITH SCHEMA
# > EVOLUTION` no MERGE — e só para mudanças compatíveis: coluna nova, e com type widening,
# > alargar tipo. Na bronze eu deixo evoluir (não perder dado da fonte); na silver e na gold o
# > schema é contrato: mudança passa por revisão, não por flag."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Onde ligar**: `.option("mergeSchema", "true")` por escrita;
#   `spark.databricks.delta.schema.autoMerge.enabled` para a sessão (este projeto deixa
#   **desligado** em `get_spark()` de propósito); `MERGE WITH SCHEMA EVOLUTION` no SQL /
#   `.withSchemaEvolution()` no Python.
# - **`overwriteSchema`**: com `mode("overwrite")`, troca o schema inteiro (inclusive tipos e
#   partição). É uma tabela nova com o mesmo nome — quebra consumidores.
# - **Evolução no streaming**: uma mudança de schema na origem para o stream; ele precisa ser
#   reiniciado (o Auto Loader faz isso de forma controlada — notebook 03).
# - **Renomear/dropar coluna** não é `mergeSchema`: exige column mapping (§8).
# - **Colunas aninhadas** (struct) evoluem com as mesmas regras; ordem de colunas é resolvida
#   por nome, não por posição, no `save`/`saveAsTable` — mas `INSERT INTO` SQL sem lista de
#   colunas é **por posição** (bug clássico).
# </details>
#
# **Trade-offs:** evolução automática em tudo = o schema vira o que a fonte quiser (erros de
# digitação viram colunas). Enforcement em tudo = pipeline parado a cada campo novo. A regra
# usual: permissivo na bronze, estrito daí para cima.

# %% [markdown]
# ## 7. Constraints e generated columns 🧪
#
# **O que é.**
# - **`NOT NULL`** e **`CHECK`**: regras gravadas na tabela e verificadas em **toda** escrita,
#   de qualquer job. Violou → a transação inteira falha.
# - **Generated column** (coluna gerada): coluna calculada por uma expressão determinística de
#   outras colunas, mantida pelo próprio Delta (ex.: `event_date` a partir de `created_at`).
#
# **Por que importa.** A regra mora junto do dado, não no código de um job: um segundo pipeline
# (ou um `INSERT` manual) não consegue burlar. E a coluna gerada resolve o problema clássico de
# particionar por data derivada sem depender de todo escritor lembrar de calculá-la.

# %%
CT = f"{D}/constraints"
spark.sql(f"""
    CREATE TABLE delta.`{CT}` (
        id STRING NOT NULL,
        type STRING,
        actor_login STRING,
        created_at TIMESTAMP,
        event_date DATE GENERATED ALWAYS AS (CAST(created_at AS DATE))
    ) USING delta PARTITIONED BY (event_date)""")
rows = sample.limit(5000).select("id", "type", "actor_login", "created_at")
rows.write.format("delta").mode("append").save(CT)  # sem event_date: o Delta calcula
spark.read.format("delta").load(CT).select("id", "created_at", "event_date").show(2)

attempt("ADD CONSTRAINT tipo_valido CHECK (type LIKE '%Event')",
        lambda: spark.sql(f"ALTER TABLE delta.`{CT}` ADD CONSTRAINT tipo_valido CHECK (type LIKE '%Event')"))
attempt("ADD CONSTRAINT sem_bot CHECK (actor_login NOT LIKE '%[bot]')  — o dado atual viola",
        lambda: spark.sql(f"ALTER TABLE delta.`{CT}` ADD CONSTRAINT sem_bot CHECK (actor_login NOT LIKE '%[bot]')"))

# %%
n_before = spark.read.format("delta").load(CT).count()
good = sample.orderBy("id").limit(100).select("id", "type", "actor_login", "created_at")
bad_type = good.limit(1).withColumn("type", F.lit("lixo")).withColumn("id", F.lit("x-1"))
bad_null = good.limit(1).withColumn("id", F.lit(None).cast("string"))
append = lambda df: df.write.format("delta").mode("append").save(CT)  # noqa: E731

attempt("1 linha com type = 'lixo' (viola o CHECK)", lambda: append(bad_type))
attempt("1 linha com id NULL (viola o NOT NULL)", lambda: append(bad_null))
attempt("event_date informada e diferente da expressão",
        lambda: append(good.limit(1).withColumn("event_date", F.lit("1999-01-01").cast("date"))))
attempt("lote de 100 linhas boas + 1 ruim", lambda: append(good.unionByName(bad_type)))
print(f"\nlinhas antes: {n_before:,} | depois das 4 tentativas: {spark.read.format('delta').load(CT).count():,}")

# %% [markdown]
# Todas recusadas, e a contagem não mudou: no lote de 100 boas + 1 ruim, **nenhuma** das 100
# entrou. É a atomicidade do §3 vista de fora — a escrita não commitou, então para a tabela
# nada aconteceu (os parquet que chegaram a ser gravados são órfãos, lixo para o VACUUM).
#
# Onde isso fica guardado, e o bônus da coluna gerada na leitura:

# %%
props = spark.sql(f"SHOW TBLPROPERTIES delta.`{CT}`").filter("key LIKE 'delta.constraints%'").collect()
print("constraint no metaData:", [(r.key, r.value) for r in props])
meta = next(a["metaData"] for a in read_commit(CT, 0) if "metaData" in a)
field = next(f for f in json.loads(meta["schemaString"])["fields"] if f["name"] == "event_date")
print("coluna gerada no schema:", field)
print("NOT NULL no schema     :", next(f for f in json.loads(meta["schemaString"])["fields"] if f["name"] == "id"))
q = spark.read.format("delta").load(CT).filter("created_at >= '2026-10-01 13:00:00'")
print("\nfiltro só por created_at →", grep_plan(q, "PartitionFilters", mode="formatted"))

# %% [markdown]
# O filtro foi escrito só sobre `created_at`, e o plano ganhou um **filtro de partição** sobre
# `event_date`: o Delta conhece a expressão geradora e deriva o filtro sozinho. Quem consulta
# não precisa saber como a tabela é particionada.
#
# **Identity column** (chave substituta numérica gerada pelo Delta) — testando no OSS 4.4:

# %%
IDT = f"{D}/identity"
attempt("CREATE TABLE com GENERATED ALWAYS AS IDENTITY", lambda: spark.sql(
    f"CREATE TABLE delta.`{IDT}` (sk BIGINT GENERATED ALWAYS AS IDENTITY, actor_login STRING) USING delta"))
for _ in range(2):  # dois lotes: os valores continuam de onde pararam
    sample.select("actor_login").distinct().orderBy("actor_login").limit(3) \
        .write.format("delta").mode("append").save(IDT)
spark.read.format("delta").load(IDT).orderBy("sk").show()

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "`NOT NULL` e `CHECK` são regras da tabela, verificadas em toda
# > escrita; se uma linha viola, o commit inteiro falha — bom para invariantes duras, tipo
# > chave não nula. Para regra de qualidade em que eu quero separar o ruim e seguir, uso
# > quarentena ou expectations, não constraint. Coluna gerada eu uso para partição derivada: o
# > Delta calcula na escrita e ainda converte o filtro da coluna original em filtro de partição."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`PRIMARY KEY` / `FOREIGN KEY`** no Unity Catalog são **informativas** (não verificadas):
#   documentam o modelo e ajudam o otimizador (`RELY`), mas não impedem duplicata ☁️. Unicidade
#   no Delta se garante com MERGE por chave, não com constraint.
# - **CHECK não pode** referenciar outra tabela nem usar função não determinística.
# - **Identity**: valores únicos e crescentes, **não** contíguos (há buracos); escrita
#   concorrente na mesma tabela com identity perde paralelismo (precisa serializar a alocação).
# - **Constraint × expectation** (notebook 08): constraint = tudo ou nada, vale para qualquer
#   escritor; expectation (Lakeflow) = warn / drop / fail por regra, com métricas.
# - **Custo**: o CHECK é avaliado em toda linha escrita — barato para expressão simples.
# </details>
#
# **Trade-offs / quando NÃO usar:** constraint que falha derruba o lote inteiro; em ingestão de
# fonte suja isso para o pipeline por causa de uma linha. Lá o certo é quarentena.

# %% [markdown]
# ## 8. Column mapping: renomear e dropar coluna sem reescrever 🧪
#
# **O que é.** Por padrão a coluna da tabela **é** a coluna do parquet (casam por nome).
# *Column mapping* separa as duas coisas: o schema do log guarda, para cada coluna lógica, um
# **nome físico** e um **id**. Renomear ou dropar passa a ser mudança só de metadados.
#
# **Por que importa.** Sem ele, `RENAME COLUMN` e `DROP COLUMN` não existem — a saída era
# reescrever a tabela inteira. Também libera nomes com espaço e caracteres que o parquet não aceita.

# %%
CM = f"{D}/colmap"
sample.limit(5000).coalesce(1).write.format("delta").save(CM)
print("protocolo antes:", protocol(CM))
attempt("RENAME COLUMN sem column mapping",
        lambda: spark.sql(f"ALTER TABLE delta.`{CM}` RENAME COLUMN repo_name TO repository"))
spark.sql(f"""ALTER TABLE delta.`{CM}` SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
attempt("RENAME COLUMN repo_name TO repository",
        lambda: spark.sql(f"ALTER TABLE delta.`{CM}` RENAME COLUMN repo_name TO repository"))
attempt("DROP COLUMN event_hour", lambda: spark.sql(f"ALTER TABLE delta.`{CM}` DROP COLUMN event_hour"))
attempt("ADD COLUMN is_bot", lambda: spark.sql(f"ALTER TABLE delta.`{CM}` ADD COLUMN is_bot BOOLEAN"))
print("protocolo depois:", protocol(CM), "\n")
show_log(CM)

# %%
parquet_file = next(p for p in os.listdir(CM) if p.endswith(".parquet"))
print("colunas da TABELA  :", spark.read.format("delta").load(CM).columns)
print("colunas do PARQUET :", spark.read.parquet(f"{CM}/{parquet_file}").columns)
last_meta = [a["metaData"] for v in range(5) for a in read_commit(CM, v) if "metaData" in a][-1]
print("\nmapa lógico → físico (no metaData):")
for f in json.loads(last_meta["schemaString"])["fields"]:
    print(f"  {f['name']:<12} id={f['metadata']['delta.columnMapping.id']}  "
          f"físico={f['metadata']['delta.columnMapping.physicalName']}")

# %% [markdown]
# Os commits de RENAME/DROP/ADD têm **zero** `add` e zero `remove`: nenhum arquivo foi tocado.
# O parquet continua com os nomes antigos (inclusive a coluna dropada, que segue **fisicamente
# lá**); a tabela mostra os nomes novos porque o leitor traduz pelo mapa. A coluna criada
# depois do mapping ganha um nome físico aleatório (`col-<uuid>`), o que impede que uma coluna
# nova chamada `event_hour` "ressuscite" o dado da antiga.
#
# > 🎤 **Resposta de 30 s:** "Column mapping desacopla o nome lógico do nome físico no parquet.
# > Com `delta.columnMapping.mode = name`, renomear e dropar coluna viram operações de
# > metadados, sem reescrita. O preço é subir o protocolo — leitor 2, escritor 5 —, então
# > clientes antigos deixam de ler a tabela; e o dado da coluna dropada continua nos arquivos
# > até um `REORG ... APPLY (PURGE)` seguido de VACUUM."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Modos**: `name` (o usual; funciona em tabela existente) e `id` (casa pelo field id do
#   parquet; usado na interoperabilidade com Iceberg).
# - **É caminho só de ida** na prática: desligar exige reescrever os arquivos.
# - **Streaming**: renomear/dropar coluna numa tabela que é **origem** de um stream quebra o
#   stream (mudança de schema não aditiva); é preciso `schemaTrackingLocation`.
# - **LGPD**: `DROP COLUMN cpf` não apaga o CPF dos parquet. Sequência completa: DROP →
#   `REORG TABLE ... APPLY (PURGE)` (reescreve sem a coluna) → `VACUUM` depois da retenção.
# - **UniForm/Iceberg** e **Liquid Clustering** em algumas versões exigem column mapping.
# </details>
#
# **Trade-offs:** ganha flexibilidade de schema, perde compatibilidade com leitores antigos e a
# legibilidade direta do parquet (quem ler o arquivo sem passar pelo Delta vê nomes físicos).

# %% [markdown]
# ## 9. Deletion vectors: apagar sem reescrever 🧪
#
# **O que é.** Um *deletion vector* (DV) é um bitmap, guardado num arquivo `.bin` ao lado dos
# dados, que diz "as linhas 17, 204 e 3.118 **deste parquet** estão apagadas". O `DELETE` deixa
# de reescrever o arquivo: grava o DV e, no log, troca o `add` do arquivo por um `add` do
# **mesmo** arquivo com o DV anexado. Quem lê aplica o filtro — é o *merge-on-read*.
#
# **Por que importa.** No copy-on-write (§1), apagar 10 linhas de um arquivo de 1 GB reescreve
# 1 GB. Com DV, grava alguns bytes. Escritas pontuais (DELETE de LGPD, UPDATE/MERGE de poucas
# linhas) ficam muito mais baratas, ao custo de um pouco mais de trabalho na leitura.
#
# **Como funciona — o que o OSS 4.4 faz em cada operação.** Duas tabelas idênticas (a hora 12
# num arquivo só), uma com `delta.enableDeletionVectors = false` e outra com `true`. As mesmas
# três operações nas duas; para cada uma, o que o commit gravou.

# %%
hour12 = sample.filter("event_hour = 12").select("id", "type", "actor_login", "repo_name", "created_at")
top3 = [r.actor_login for r in hour12.groupBy("actor_login").count()
        .orderBy(F.desc("count"), "actor_login").limit(3).collect()]
merge_src = (hour12.orderBy("id").limit(20).withColumn("repo_name", F.lit("corrigido/pelo-merge"))
             .unionByName(sample.filter("event_hour = 13").orderBy("id").limit(5)
                          .select("id", "type", "actor_login", "repo_name", "created_at"))).cache()


def make_dv_table(name: str, enabled: bool) -> str:
    path = f"{D}/{name}"
    spark.sql(f"""CREATE TABLE delta.`{path}` (id STRING, type STRING, actor_login STRING, repo_name STRING,
                  created_at TIMESTAMP) USING delta
                  TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(enabled).lower()}')""")
    hour12.coalesce(1).write.format("delta").mode("append").save(path)
    return path


OPS = {
    f"DELETE (ator {top3[1]})": lambda p: spark.sql(f"DELETE FROM delta.`{p}` WHERE actor_login = '{top3[1]}'"),
    f"UPDATE (ator {top3[2]})": lambda p: spark.sql(
        f"UPDATE delta.`{p}` SET repo_name = upper(repo_name) WHERE actor_login = '{top3[2]}'"),
    "MERGE (20 updates + 5 inserts)": lambda p: (
        DeltaTable.forPath(spark, p).alias("t").merge(merge_src.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()),
}
NODV, DV = make_dv_table("dv_off", False), make_dv_table("dv_on", True)
print(f"arquivo inicial: {detail(NODV)} | linhas: {hour12.count():,}\n")
dv_rows = []
for label, op in OPS.items():
    for name, path in (("sem DV", NODV), ("com DV", DV)):
        op(path)
        c = log_summary(path, last=1)[0]
        dv_rows.append({"operação": label, "tabela": name, "add": c.files_added, "remove": c.files_removed,
                        "add c/ DV": c.adds_with_dv, "KB de parquet novo": round(c.bytes_new / 1024, 1)})
print(as_table(dv_rows))
same = (spark.read.format("delta").load(NODV).exceptAll(spark.read.format("delta").load(DV)).count()
        + spark.read.format("delta").load(DV).exceptAll(spark.read.format("delta").load(NODV)).count())
print(f"\nlinhas diferentes entre as duas tabelas: {same}")
print("no disco — sem DV:", physical_files(NODV), "| com DV:", physical_files(DV))

# %% [markdown]
# Como ler a tabela (as duas tabelas terminam com conteúdo idêntico):
#
# - **Sem DV**, toda operação reescreve o arquivo inteiro: `remove` do antigo + `add` de um
#   novo praticamente do mesmo tamanho (coluna `KB de parquet novo`), mesmo para mexer em
#   poucas linhas.
# - **Com DV**, o `DELETE` não grava parquet nenhum (0 KB): o `add` é o **mesmo arquivo**, agora
#   com DV (coluna `add c/ DV`) — por isso há 1 `remove` e 1 `add` do mesmo caminho. `UPDATE` e
#   `MERGE` marcam as linhas antigas como apagadas no DV e gravam um segundo parquet,
#   **pequeno**, só com as linhas novas/alteradas.
# - No disco: a tabela sem DV acumulou uma cópia inteira por operação; a com DV, o arquivo
#   original, dois arquivos pequenos e três `.bin` de DV.
#
# Os três casos (DELETE, UPDATE e MERGE) usaram DV neste Delta OSS 4.4.
#
# O descritor do DV dentro da ação `add` e o que o leitor faz com ele:

# %%
dv_add = next(a["add"] for v in (4, 3, 2) for a in read_commit(DV, v) if a.get("add", {}).get("deletionVector"))
print("add.deletionVector:", dv_add["deletionVector"])
print("stats do mesmo add :", {k: v for k, v in json.loads(dv_add["stats"]).items() if k in ("numRecords", "tightBounds")})
print("protocolo sem DV:", protocol(NODV))
print("protocolo com DV:", protocol(DV))

# %% [markdown]
# `cardinality` é o número de linhas apagadas naquele arquivo; `numRecords` continua sendo o
# total **físico** do parquet (linhas vivas = `numRecords − cardinality`); `tightBounds: false`
# avisa que min/max podem incluir linhas apagadas (continuam válidos para pular arquivo, mas
# não servem mais para responder `MIN`/`MAX` só com metadados).
#
# **Materializar os DVs.** As linhas apagadas continuam no parquet. Para tirá-las de fato
# (LGPD, ou porque a leitura está pagando filtro demais): `REORG TABLE ... APPLY (PURGE)`.

# %%
spark.sql(f"REORG TABLE delta.`{DV}` APPLY (PURGE)").collect()
show_log(DV, last=1)
print("ativos:", detail(DV), "| no disco:", physical_files(DV))

# %% [markdown]
# O `REORG` reescreveu o arquivo que tinha DV: 1 `remove` (arquivo + DV) e 1 `add` de um parquet
# novo, já sem as linhas apagadas e sem DV, com `dataChange = False` (o conteúdo da tabela é o
# mesmo). O arquivo antigo e os `.bin` continuam no disco — só o VACUUM, depois da retenção, os
# apaga.
#
# > 🎤 **Resposta de 30 s:** "Deletion vector é um bitmap de linhas apagadas por arquivo. Com
# > ele, DELETE, UPDATE e MERGE param de reescrever o parquet inteiro: marcam as linhas velhas
# > no DV e gravam só as linhas novas. Escrita pontual fica muito mais barata; a leitura paga
# > um filtro a mais, e o OPTIMIZE ou o `REORG PURGE` consolidam depois. No Databricks vem
# > ligado por padrão em tabelas novas e é a base da concorrência por linha. O cuidado é o
# > protocolo — leitor 3, escritor 7 — e LGPD: a linha 'apagada' ainda está no arquivo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Formato**: RoaringBitmap serializado; DVs pequenos podem ir *inline* no próprio log
#   (`storageType: "i"`), os demais num `.bin` (`"u"`), vários DVs por arquivo.
# - **Quem aciona a consolidação**: `OPTIMIZE` reescreve arquivos cujo DV passou de um limiar;
#   auto compaction e Predictive Optimization fazem isso sozinhos no Databricks ☁️.
# - **Photon/Databricks** ☁️ aplica DV em MERGE há mais tempo e com *low-shuffle merge*; no OSS
#   o suporte chegou por etapas (DELETE no 2.4, UPDATE no 3.0, MERGE no 3.1).
# - **Iceberg** tem o mesmo conceito: *position deletes* (v2) e deletion vectors (v3, formato
#   binário compatível com o do Delta).
# - **Quando DV perde**: operação que altera uma fração grande do arquivo — reescrever de uma
#   vez é melhor que DV + arquivo novo + consolidação depois.
# </details>
#
# **Trade-offs / quando NÃO usar:** tabelas lidas por clientes que não entendem DV (leitores
# antigos, alguns conectores) — eles **não conseguem** ler a tabela; e cargas que só fazem
# append ou overwrite não ganham nada.

# %% [markdown]
# ## 10. Change Data Feed (CDF) alimentando um consumidor incremental 🧪
#
# **O que é.** Com `delta.enableChangeDataFeed = true`, o Delta passa a registrar **as linhas
# que mudaram** em cada commit, com o tipo da mudança: `insert`, `delete`, `update_preimage`
# (valor antes) e `update_postimage` (valor depois). É o CDC (*change data capture*) da
# própria tabela Delta.
#
# **Por que importa.** O consumidor da tabela (a próxima camada, uma réplica, um índice, um
# sistema externo) processa **só o que mudou** em vez de reler tudo. É o que torna barato o
# incremental silver → gold quando a silver sofre UPDATE/DELETE — caso em que "ler só os
# arquivos novos" não funciona.
#
# **Como funciona.** Uma tabela de origem com CDF e um consumidor
# (`oss_lakehouse.delta_log.apply_changes`) que mantém uma réplica: guarda num arquivo de
# estado a última versão aplicada, lê o feed dali em diante, reduz a uma mudança por chave e
# aplica com MERGE.

# %%
SRC, DST, STATE = f"{D}/cdf_origem", f"{D}/cdf_replica", f"{D}/cdf_estado.json"
spark.sql(f"""CREATE TABLE delta.`{SRC}` (id STRING, type STRING, actor_login STRING, repo_name STRING)
              USING delta TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")                 # versão 0
src_rows = sample.filter("event_hour = 12").orderBy("id").select("id", "type", "actor_login", "repo_name")
src_rows.limit(2000).coalesce(1).write.format("delta").mode("append").save(SRC)                     # versão 1

run1 = apply_changes(spark, SRC, DST, key="id", state_file=STATE)
print("rodada 1:", run1, "| réplica:", spark.read.format("delta").load(DST).count(), "linhas")

# %% [markdown]
# A primeira rodada leu as versões 0–1 (a carga inicial chega como `insert`). Agora a origem
# sofre um UPDATE, um DELETE e um MERGE (que atualiza e insere):

# %%
spark.sql(f"UPDATE delta.`{SRC}` SET repo_name = upper(repo_name) WHERE type = 'IssuesEvent'")      # versão 2
spark.sql(f"DELETE FROM delta.`{SRC}` WHERE type = 'ForkEvent'")                                    # versão 3
news = (sample.filter("event_hour = 13").orderBy("id").limit(50).select("id", "type", "actor_login", "repo_name")
        .unionByName(src_rows.limit(30).withColumn("repo_name", F.lit("corrigido/pelo-merge"))))
(DeltaTable.forPath(spark, SRC).alias("t").merge(news.alias("s"), "t.id = s.id")
 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())                                       # versão 4

feed = read_changes(spark, SRC, start=2)
feed.groupBy("_commit_version", "_change_type").count().orderBy("_commit_version", "_change_type").show()
one = feed.filter("_change_type LIKE 'update%' AND _commit_version = 2").orderBy("id", "_change_type").limit(2)
one.select("id", "repo_name", "_change_type", "_commit_version").show(truncate=40)
show_log(SRC)
print("\nno disco:", physical_files(SRC))

# %% [markdown]
# O feed tem uma linha por mudança, com o par pré/pós-imagem em cada update. No log, os commits
# de UPDATE, DELETE e MERGE ganharam uma ação **`cdc`** cada: um parquet extra na pasta
# `_change_data/` com as linhas alteradas (é o custo de escrita do CDF). O append (versão 1)
# não precisa: as linhas inseridas são o próprio arquivo do `add`, e o feed é derivado dele.
#
# O consumidor roda de novo e lê **só** as versões 2–4:

# %%
run2 = apply_changes(spark, SRC, DST, key="id", state_file=STATE)
print("rodada 2:", run2)
src_df, dst_df = spark.read.format("delta").load(SRC), spark.read.format("delta").load(DST)
print(f"origem: {src_df.count():,} linhas | réplica: {dst_df.count():,} linhas | "
      f"diferença: {src_df.exceptAll(dst_df).count() + dst_df.exceptAll(src_df).count()} linhas")
run3 = apply_changes(spark, SRC, DST, key="id", state_file=STATE)
print("rodada 3 (nada novo):", run3, "| pulou =", run3.skipped)
print("estado gravado:", open(STATE).read())

# %% [markdown]
# A réplica ficou idêntica à origem lendo só as mudanças; `changes_read` é o número de linhas
# do feed e `keys_applied` o de chaves distintas depois de reduzir (um update conta 2 linhas no
# feed — pré e pós — e 1 chave; uma chave alterada em dois commits também conta uma vez só).
# Sem nada novo, a rodada 3 não lê nem escreve.
#
# > 🎤 **Resposta de 30 s:** "Change Data Feed é o CDC da tabela Delta: com a propriedade
# > ligada, cada commit registra as linhas inseridas, apagadas e o antes/depois dos updates.
# > O consumidor lê a partir da última versão processada — em batch com `startingVersion` ou em
# > streaming com checkpoint — e aplica com MERGE por chave, que é idempotente. Uso para
# > propagar UPDATE/DELETE entre camadas sem reprocessar a tabela inteira."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Versão em streaming** (o caminho usual em produção; o checkpoint substitui o arquivo de
#   estado — não executado aqui, o notebook 06 cobre Structured Streaming):
#   ```python
#   (spark.readStream.format("delta").option("readChangeFeed", "true").load(SRC)
#         .writeStream.foreachBatch(lambda df, _: merge_latest(df))   # MERGE por chave
#         .option("checkpointLocation", ckpt).trigger(availableNow=True).start())
#   ```
# - **Por que reduzir a uma linha por chave**: num intervalo de versões a mesma chave pode
#   mudar várias vezes; MERGE com duas linhas da origem casando com uma do destino falha
#   (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`).
# - **CDF não é retroativo**: só registra a partir do commit em que foi ligado. E os arquivos de
#   `_change_data` seguem a retenção do VACUUM — consumidor parado por mais de 7 dias perde
#   mudanças e precisa de carga completa.
# - **Custo**: UPDATE/MERGE gravam os arquivos `cdc` a mais. Em tabela com escrita pesada e
#   nenhum consumidor de mudanças, não ligar.
# - **Alternativas**: stream direto da tabela Delta (só serve se a origem é append-only —
#   UPDATE/DELETE quebram o stream sem `skipChangeCommits`); no Databricks, `AUTO CDC` /
#   `APPLY CHANGES` do Lakeflow faz o MERGE e o SCD por você ☁️.
# </details>
#
# **Trade-offs:** CDF dá o "o que mudou", não o "por quê"; e acopla o consumidor ao histórico
# de versões da origem — um `RESTORE` ou uma recarga completa na origem chega como uma enxurrada
# de mudanças.

# %% [markdown]
# ## 11. Protocolo e table features 🧪
#
# **O que é.** O `protocol` do log diz o que um cliente precisa saber fazer: `minReaderVersion`
# para ler, `minWriterVersion` para escrever. Antigamente cada número de versão trazia um
# **pacote** de recursos; desde o leitor 3 / escritor 7, o protocolo lista **table features**
# nominais (`deletionVectors`, `columnMapping`, `changeDataFeed`…) e o cliente só precisa
# suportar as que a tabela usa.
#
# **Por que importa.** Ligar um recurso pode **subir o protocolo** e deixar de fora clientes
# mais antigos (um Spark velho, um conector de BI, um leitor em Rust/Python). É a pergunta
# a fazer antes de ligar qualquer feature: quem mais lê esta tabela?
#
# **Como funciona.** O protocolo de cada tabela criada neste notebook:

# %%
tables = {"events (particionada)": EV, "schema (type widening)": ST, "constraints (CHECK + gerada)": CT,
          "colmap (column mapping)": CM, "dv_off": NODV, "dv_on (deletion vectors)": DV, "cdf_origem (CDF)": SRC,
          "identity": IDT}
print(as_table([{"tabela": name, **protocol(path)} for name, path in tables.items()]))

# %% [markdown]
# Como ler:
#
# - A tabela simples fica em leitor 1 / escritor 2 — qualquer cliente Delta lê.
# - CHECK, coluna gerada, CDF e identity sobem **só o escritor** (para 7, com a feature
#   nominal): o leitor continua 1, clientes antigos **leem** normalmente.
# - Type widening e deletion vectors sobem **o leitor** para 3: quem não conhece a feature não
#   lê a tabela.
# - Column mapping ficou em leitor 2 / escritor 5 porque foi isso que o §8 pediu à mão — e, no
#   esquema legado, a versão 5 traz o **pacote inteiro** das anteriores: a tabela lista
#   `changeDataFeed`, `checkConstraints` e `generatedColumns` sem usar nenhuma delas. É o
#   problema que as table features resolveram.
# - `appendOnly` e `invariants` aparecem em todas: fazem parte do escritor 2.
#
# Dá para **remover** uma feature? Testando com os deletion vectors (a tabela já passou pelo
# `REORG PURGE` do §9, então não há mais nenhum DV ativo):

# %%
attempt("ALTER TABLE dv_on DROP FEATURE deletionVectors",
        lambda: spark.sql(f"ALTER TABLE delta.`{DV}` DROP FEATURE deletionVectors"))
print("protocolo de dv_on agora:", protocol(DV))

# %% [markdown]
# Funcionou: o leitor voltou a 1. No lugar de `deletionVectors` entrou `checkpointProtection`,
# uma feature **de escritor** que o Delta acrescenta ao remover uma feature de leitura: ela
# protege o histórico (os commits antigos, em que os DVs ainda existiam) para que um cliente
# antigo não seja levado a ler versões que ele não entende. Só foi simples assim porque os DVs
# já tinham sido materializados; com DV ativo, o comando exige o `REORG PURGE` antes.
#
# > 🎤 **Resposta de 30 s:** "O protocolo é o contrato de compatibilidade da tabela: versão
# > mínima de leitor e de escritor e, nas tabelas modernas, uma lista de table features. Ligar
# > deletion vectors, column mapping ou clustering sobe o protocolo, e cliente que não suporta
# > é barrado em vez de ler errado. Antes de ligar um recurso eu confiro quem consome a tabela
# > — principalmente leitores fora do Spark."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Mapa das versões legadas**: escritor 2 = append-only e invariantes; 3 = CHECK; 4 = CDF e
#   generated columns; 5 = column mapping (com leitor 2); 6 = identity; 7 = table features
#   (com leitor 3 quando a feature afeta leitura). Na tabela acima, o Delta 4.4 pulou direto
#   para o escritor 7 com a feature nominal em vez de usar os números 3, 4 e 6.
# - **Feature de escritor × de leitor**: CDF, CHECK e identity só exigem do **escritor** (o
#   leitor antigo lê normalmente); deletion vectors e column mapping exigem do **leitor** também.
# - **Subir é fácil, descer dá trabalho**: `ALTER TABLE ... DROP FEATURE` precisa que não
#   reste vestígio da feature nos arquivos ativos, e o histórico antigo continua contendo a
#   feature — daí o `checkpointProtection` (ou, na variante `TRUNCATE HISTORY`, esperar a
#   retenção e cortar o histórico). Nem toda feature pode ser removida.
# - **Padrões diferentes por plataforma**: o que o Databricks liga sozinho em tabela nova
#   (deletion vectors, e outros conforme o runtime) não é o padrão do OSS — uma tabela criada
#   no Databricks pode não abrir num cliente OSS antigo.
# - `delta.minReaderVersion`/`minWriterVersion` em `TBLPROPERTIES` sobem o protocolo à mão;
#   `DeltaTable.upgradeTableProtocol` / `addFeatureSupport` fazem o mesmo pela API.
# </details>
#
# **Trade-offs:** cada feature é um ganho local e um custo de interoperabilidade. Tabela
# consumida só dentro do Databricks: ligue o que ajudar. Tabela compartilhada com outros
# motores: escolha pelo denominador comum (ou exponha via UniForm / Delta Sharing ☁️).

# %% [markdown]
# ## 12. OPTIMIZE visto pelo log 🧪
#
# **O que é.** O notebook 09 (§6) mediu o ganho de compactar small files. Aqui interessa o que
# o `OPTIMIZE` **grava**: um commit que remove N arquivos e adiciona poucos, todos com
# `dataChange: false` — "o conteúdo da tabela é o mesmo, só mudei a organização".
#
# **Por que importa.** Esse flag é o que permite compactar uma tabela em uso: leitores de
# streaming e de CDF **ignoram** o commit (não há linha nova para processar) e o OPTIMIZE não
# conflita com appends concorrentes.

# %%
OT = f"{D}/optimize"
sample.repartition(40).write.format("delta").save(OT)
spark.sql(f"OPTIMIZE delta.`{OT}`").collect()
show_log(OT)
commit = read_commit(OT, 1)
info = next(a["commitInfo"] for a in commit if "commitInfo" in a)
rm = next(a["remove"] for a in commit if "remove" in a)
print("\ncommitInfo.operationMetrics:", {k: info["operationMetrics"][k] for k in
                                         ("numRemovedFiles", "numAddedFiles", "numRemovedBytes", "numAddedBytes")})
print("um remove:", {k: rm[k] for k in ("dataChange", "deletionTimestamp", "size")})
print("ativos:", detail(OT), "| no disco:", physical_files(OT))

# %% [markdown]
# O commit do OPTIMIZE tem 40 `remove` e 1 `add`, com `dataChange = False`. Cada `remove`
# carrega o `deletionTimestamp` — é a partir dele que o VACUUM conta a retenção (§5). E o disco
# continua com os 40 arquivos pequenos **mais** o compactado: OPTIMIZE aumenta o uso de storage
# até o VACUUM passar.
#
# > 🎤 **Resposta de 30 s:** "Para o log, OPTIMIZE é um commit com `remove` dos arquivos
# > pequenos e `add` dos compactados, marcado com `dataChange=false`. Por isso é seguro rodar
# > com a tabela em uso: leitores seguem no snapshot deles, streams ignoram o commit, appends
# > não conflitam. O que ele não faz é liberar espaço — isso é o VACUUM, depois da retenção."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Com o que o OPTIMIZE conflita**: UPDATE/DELETE/MERGE que removam os mesmos arquivos
#   (`ConcurrentDeleteReadException`/`ConcurrentDeleteDeleteException`). Com deletion vectors e
#   row-level concurrency no Databricks ☁️ o conflito diminui.
# - **`ZORDER BY` e Liquid Clustering** são o mesmo tipo de commit (`dataChange=false`), só que
#   o conteúdo dos arquivos novos sai ordenado/agrupado.
# - **CDF**: commit com `dataChange=false` não gera mudança no feed.
# - **Tamanho-alvo**: `delta.targetFileSize` (ou o ajuste automático do Databricks pelo tamanho
#   da tabela); `spark.databricks.delta.optimize.maxFileSize` no OSS (1 GB).
# </details>
#
# **Trade-offs:** OPTIMIZE gasta compute e dobra temporariamente o storage dos arquivos
# reescritos; em tabela pequena ou que já nasce com arquivos grandes, é custo sem retorno.

# %% [markdown]
# ## 13. Delta × Iceberg × Hudi (e UniForm ☁️)
#
# **O que é.** Os três são *open table formats*: uma camada de metadados sobre arquivos
# parquet que dá transação, evolução de schema e time travel. A diferença está em **onde fica
# o metadado** e **como se faz o commit**.
#
# | | Delta Lake | Apache Iceberg | Apache Hudi |
# |---|---|---|---|
# | Metadado | Log ordenado de commits JSON + checkpoints parquet, na pasta da tabela (§1–§2) | Árvore: `metadata.json` → *manifest list* (por snapshot) → *manifests* (listas de arquivos com stats) | *Timeline* de ações (`.hoodie/`) + tabela de metadados interna |
# | Commit atômico | Criar o próximo arquivo do log (put-if-absent) ou via catálogo | Trocar, no **catálogo**, o ponteiro para o novo `metadata.json` (compare-and-swap) | Marcar a ação como completa na timeline |
# | Papel do catálogo | Opcional no OSS (a tabela é o caminho); central no Unity Catalog | **Obrigatório** — é quem garante o commit | Opcional |
# | Particionamento | Colunas explícitas + generated columns; Liquid Clustering | *Hidden partitioning* (transformações `day(ts)`, `bucket(n, id)`) e *partition evolution* | Explícito; índices por registro |
# | Alteração pontual | Copy-on-write ou deletion vectors (§9) | Copy-on-write, *position/equality deletes* (v2), deletion vectors (v3) | Copy-on-write **ou** merge-on-read (logs de delta por arquivo base + compactação) |
# | Ponto forte | Integração com Spark/Databricks, streaming, CDF, simplicidade operacional | Neutralidade de motor (Trino, Flink, Snowflake, BigQuery, Athena…), evolução de partição | Upsert de alta frequência e ingestão CDC com índice por chave |
#
# **Por que importa.** A pergunta de entrevista raramente é "qual é melhor" — é "por que
# Delta aqui?" e "como outro motor lê isso?". Em 2026 os formatos convergiram bastante
# (deletion vectors, row tracking e tipo `VARIANT` existem no Delta e no Iceberg v3), e a
# disputa passou do formato para o **catálogo** (Unity Catalog, Polaris, Glue…).
#
# **UniForm ☁️** (Universal Format). Uma tabela Delta que publica **também** metadados Iceberg
# apontando para os **mesmos** parquet. Clientes Iceberg leem pelo endpoint Iceberg REST do
# Unity Catalog; a escrita continua sendo Delta.
#
# ```sql
# -- ☁️ Databricks / Unity Catalog (não executado aqui)
# CREATE TABLE main.gold.gh_daily (...)
# TBLPROPERTIES (
#   'delta.columnMapping.mode'               = 'name',     -- pré-requisito (§8)
#   'delta.enableIcebergCompatV2'            = 'true',
#   'delta.universalFormat.enabledFormats'   = 'iceberg'
# );
# -- Tabela já existente: REORG TABLE ... APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION = 2));
# ```
#
# O Delta OSS também tem UniForm (módulo `delta-iceberg`, com Hive Metastore como catálogo);
# não está instalado neste ambiente, por isso fica marcado como ☁️ e não é demonstrado.
#
# > 🎤 **Resposta de 30 s:** "Os três resolvem o mesmo problema — ACID e evolução sobre parquet.
# > Delta usa um log ordenado na pasta da tabela; Iceberg, uma árvore de metadados com o commit
# > feito no catálogo; Hudi, uma timeline com foco em upsert. Eu escolho pelo ecossistema: em
# > Databricks, Delta é o nativo e o mais bem integrado; se outros motores precisam ler, exponho
# > via UniForm/Iceberg REST do Unity Catalog em vez de duplicar dado. A diferença técnica
# > entre os formatos hoje é menor que a diferença entre os catálogos."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Limites do UniForm**: leitura apenas (clientes Iceberg não escrevem na tabela Delta); a
#   geração do metadado Iceberg é assíncrona (pequena defasagem); exige column mapping e
#   restringe algumas features — conferir a documentação da versão do runtime antes de ligar.
# - **Tabelas Iceberg gerenciadas no Unity Catalog** ☁️: o caminho inverso — a tabela nasce
#   Iceberg e o Databricks lê/escreve. Útil quando o padrão da empresa é Iceberg.
# - **Delta Sharing** é outra coisa: protocolo para compartilhar tabelas **entre organizações**
#   sem copiar dado (o receptor não precisa de Databricks).
# - **Delta Kernel**: biblioteca (Java/Rust) que implementa o protocolo para outros motores não
#   precisarem reimplementar o log replay — é como Trino, Flink e DuckDB leem Delta.
# - **Migração**: `CONVERT TO DELTA` (parquet/Iceberg → Delta, in place, só cria o log);
#   `CLONE` de tabela Iceberg/parquet no Databricks ☁️.
# </details>
#
# **Trade-offs:** padronizar num formato reduz custo operacional; a interoperabilidade (UniForm)
# cobra em features bloqueadas e em mais uma coisa para monitorar.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# **O que muda em relação a este notebook.**
#
# | Aqui (Delta OSS local) | Databricks + Unity Catalog na Azure |
# |---|---|
# | Tabela = caminho (`delta.\`/caminho\``) | Tabela **gerenciada** pelo UC: `catalogo.schema.tabela`; o caminho no ADLS é detalhe interno |
# | Commit = criar arquivo no `_delta_log` | No ADLS Gen2, rename atômico; em tabela gerenciada, o commit é coordenado pelo catálogo |
# | Deletion vectors desligados por padrão | Ligados por padrão em tabelas novas; base da *row-level concurrency* |
# | `OPTIMIZE`/`VACUUM` agendados por você | **Predictive Optimization** roda OPTIMIZE, VACUUM e ANALYZE sozinha em tabelas gerenciadas |
# | Checkpoint a cada 10 commits | Intervalo e formato ajustados pela plataforma |
# | Histórico em `DESCRIBE HISTORY` | Idem, mais auditoria e *lineage* no UC (`system.access.audit`, `system.access.table_lineage`) |
#
# ```sql
# -- Tabela gerenciada com CDF e retenções explícitas
# CREATE TABLE main.silver.gh_events (...)
# CLUSTER BY (event_date, repo_id)                       -- Liquid Clustering (notebook 09 §7)
# TBLPROPERTIES (
#   'delta.enableChangeDataFeed'         = 'true',
#   'delta.deletedFileRetentionDuration' = 'interval 7 days',   -- janela de time travel/recuperação
#   'delta.logRetentionDuration'         = 'interval 30 days'
# );
#
# -- O que mudou desde a versão 120 (CDF por SQL)
# SELECT * FROM table_changes('main.silver.gh_events', 120);
#
# -- Voltar um erro operacional e conferir
# RESTORE TABLE main.silver.gh_events TO VERSION AS OF 118;
# DESCRIBE HISTORY main.silver.gh_events LIMIT 5;
#
# -- Recuperar uma tabela gerenciada dropada por engano (dentro da janela de retenção do UC)
# UNDROP TABLE main.silver.gh_events;
#
# -- Ambiente de teste sem copiar dado
# CREATE TABLE dev.silver.gh_events SHALLOW CLONE main.silver.gh_events;
# ```
#
# **Azure, especificamente.**
# - **ADLS Gen2 com namespace hierárquico** é pré-requisito prático: é o rename atômico dele que
#   sustenta o commit em tabelas externas.
# - **Não use lifecycle management do Storage para "limpar arquivo antigo"** em contêiner com
#   Delta: ele não conhece o log e apaga parquet ativo. Quem limpa é o VACUUM.
# - **Soft delete / versionamento de blob** dão uma segunda rede de proteção contra exclusão
#   acidental, com custo de storage; não substituem a retenção do Delta.
# - **Acesso externo** (Synapse, Fabric, Trino): confira o protocolo da tabela (§11). O Fabric
#   lê Delta via *shortcut* do OneLake; features novas podem não ser suportadas lá.
#
# **LGPD na prática** (direito à eliminação), juntando as peças deste notebook:
#
# ```sql
# DELETE FROM main.silver.gh_events WHERE actor_login = 'fulano';   -- sai do estado atual
# REORG TABLE main.silver.gh_events APPLY (PURGE);                  -- tira dos parquet (DV / coluna dropada)
# VACUUM main.silver.gh_events;                                     -- apaga os arquivos antigos, após a retenção
# -- e propagar o DELETE para as camadas seguintes (CDF) e para cópias/clones.
# ```

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. O que é uma tabela Delta, fisicamente?**
# <details><summary>Resposta</summary>Arquivos parquet + a pasta `_delta_log` com um JSON por
# commit. Cada JSON tem ações (`add`, `remove`, `metaData`, `protocol`, `commitInfo`); o estado
# da tabela é o replay delas. A cada 10 commits, um checkpoint parquet consolida o estado (§1, §2).</details>
#
# **2. Como o Delta garante ACID em cima de object storage?**
# <details><summary>Resposta</summary>Atomicidade: o commit é a criação atômica de um arquivo do
# log (put-if-absent). Isolamento: leitores fixam uma versão (snapshot); escritores usam
# concorrência otimista e validam no commit. Consistência: enforcement de schema e constraints.
# Durabilidade: o storage (§1, §3).</details>
#
# **3. Dois jobs escrevem na mesma tabela ao mesmo tempo. O que acontece?**
# <details><summary>Resposta</summary>Não há lock. Quem commita depois confere se o vencedor
# mexeu no que ele leu: se não, recommita sozinho; se sim, falha com
# `ConcurrentAppendException`/`ConcurrentDeleteReadException`. Appends cegos nunca conflitam.
# Evita-se com partições disjuntas + partição explícita na condição; trata-se com retry (§3).</details>
#
# **4. Meu MERGE falha com `ConcurrentAppendException` mesmo atualizando linhas diferentes do
# outro job. Por quê?**
# <details><summary>Resposta</summary>A condição do MERGE não restringe a partição, então ele
# "leu a tabela inteira"; qualquer arquivo novo de outro job conflita. Conserto: incluir a
# coluna de partição na condição (`AND t.event_hour = 12`). No Databricks, row-level concurrency
# reduz o problema ☁️ (§3).</details>
#
# **5. O que o VACUUM faz e por que a retenção padrão é de 7 dias?**
# <details><summary>Resposta</summary>Apaga do storage arquivos fora da versão atual e mais
# velhos que a retenção. 7 dias protegem leitores longos e escritas não commitadas (retenção 0
# pode apagar arquivo de transação em andamento e corromper a tabela) e dão janela de time
# travel. Depois dele, ler a versão antiga falha (§5).</details>
#
# **6. Time travel substitui backup?**
# <details><summary>Resposta</summary>Não. Depende dos arquivos antigos e do log, que somem com
# VACUUM (7 dias) e com a limpeza do log (30 dias); e não protege contra perda do storage ou
# exclusão da pasta. Serve para auditoria, reprodutibilidade e desfazer erro recente com
# `RESTORE` (§4, §5).</details>
#
# **7. Diferença entre schema enforcement e schema evolution?**
# <details><summary>Resposta</summary>Enforcement: escrita com schema diferente falha (padrão).
# Evolution: opt-in (`mergeSchema`, `WITH SCHEMA EVOLUTION`) para mudança compatível — coluna
# nova, tipo mais largo com type widening. Renomear/dropar exige column mapping. Permissivo na
# bronze, estrito na silver/gold (§6, §8).</details>
#
# **8. O que são deletion vectors e o que mudam?**
# <details><summary>Resposta</summary>Bitmap de linhas apagadas por arquivo. DELETE/UPDATE/MERGE
# deixam de reescrever o parquet inteiro: marcam no DV e gravam só as linhas novas
# (merge-on-read). Escrita pontual barata, leitura com um filtro a mais, protocolo leitor 3 /
# escritor 7, e a linha segue no arquivo até `REORG PURGE` + VACUUM (§9).</details>
#
# **9. Como propagar UPDATE e DELETE da silver para a gold sem reprocessar tudo?**
# <details><summary>Resposta</summary>Change Data Feed: ler as mudanças desde a última versão
# processada (batch com `startingVersion` ou streaming com checkpoint), reduzir a uma linha por
# chave e aplicar com MERGE idempotente (§10).</details>
#
# **10. Como apagar definitivamente os dados de uma pessoa (LGPD)?**
# <details><summary>Resposta</summary>`DELETE` tira do estado atual, mas o dado segue nos
# arquivos antigos (time travel) e, com DV, no próprio arquivo. Sequência: DELETE →
# `REORG ... APPLY (PURGE)` → `VACUUM` após a retenção — e propagar para camadas derivadas,
# clones e CDF (§5, §8, §9).</details>
#
# **11. O que é o protocolo da tabela e por que eu deveria me preocupar ao ligar uma feature?**
# <details><summary>Resposta</summary>`minReaderVersion`/`minWriterVersion` + table features: o
# que o cliente precisa suportar. Ligar DV ou column mapping sobe o protocolo de leitura e barra
# clientes antigos; voltar atrás é difícil. Conferir quem lê a tabela antes (§11).</details>
#
# **12. Delta ou Iceberg?**
# <details><summary>Resposta</summary>Mesmo problema, mecanismos diferentes (log ordenado ×
# árvore de metadados com commit no catálogo). Em Databricks, Delta é o nativo; para outros
# motores, UniForm/Iceberg REST do Unity Catalog expõe a mesma tabela sem duplicar dado. A
# decisão hoje é mais de ecossistema e catálogo que de formato (§13).</details>

# %% [markdown]
# ## Resumo
#
# - **Delta = parquet + log.** Cada commit é um JSON com `add`/`remove`/`metaData`/`protocol`;
#   o estado é o replay, acelerado por checkpoints. `remove` não apaga arquivo — daí o time travel.
# - **Concorrência é otimista:** sem lock; conflito aparece no commit como exceção. Partições
#   disjuntas + partição explícita na condição evitam; retry de operação idempotente trata.
# - **Só o VACUUM destrói dado.** Retenção de 7 dias protege leitores, escritores e a
#   recuperação; nunca desligar a checagem em produção. LGPD = DELETE + PURGE + VACUUM.
# - **Regras moram na tabela:** enforcement de schema, `NOT NULL`, `CHECK`, coluna gerada —
#   valem para qualquer escritor; evolução é opt-in.
# - **Recursos modernos sobem o protocolo:** column mapping (rename/drop sem reescrita),
#   deletion vectors (alteração pontual barata), CDF (incremental com UPDATE/DELETE). Antes de
#   ligar, saber quem lê a tabela.

# %%
spark.stop()
