# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 03 · Ingestão incremental de arquivos: landing → bronze, checkpoint e Auto Loader
#
# > Prova que a ingestão processa **cada arquivo uma vez**: arquivo novo é lido sozinho, re-rodar não
# > duplica, o checkpoint guarda o que já foi visto — e mostra onde isso **deixa de valer** (checkpoint
# > perdido, registro corrompido lido em silêncio) e como o Auto Loader resolve no Databricks.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Python avançado | download idempotente/atômico com retry (`oss_lakehouse.sources.gharchive`) |
# | Databricks e processamento de dados | Auto Loader (`cloudFiles`), schema evolution, rescued data ☁️ |
# | Microsoft Azure | file notification com Event Grid + fila do Storage ☁️, ADLS Gen2 |
# | Arquitetura de pipelines | checkpoint, exactly-once, backfill, COPY INTO × Auto Loader |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup

# %%
import inspect
import json
import shutil
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from oss_lakehouse.bronze import GH_EVENT_SCHEMA, ingest_gharchive_bronze
from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.sources import gharchive
from oss_lakehouse.spark import get_spark

spark = get_spark("03")
s = get_settings()
RAW_CACHE = Path(s.data_root) / "raw_cache" / "gharchive"
DEMO = Path(s.data_root) / "demo" / "03"          # landing, bronze e checkpoint PRÓPRIOS desta demo
shutil.rmtree(DEMO, ignore_errors=True)
LANDING, BRONZE, CKPT = DEMO / "landing", str(DEMO / "bronze"), str(DEMO / "_checkpoint")
LANDING.mkdir(parents=True)


def rel(p) -> str:
    return str(p).replace(str(PROJECT_ROOT) + "/", "")

# %% [markdown]
# ## 1. Download idempotente e atômico
#
# **O que é** — a fonte (GH Archive) publica um `JSON.gz` por hora em `https://data.gharchive.org/2026-10-01-15.json.gz`
# (hora **sem** zero à esquerda). O download leva cada arquivo para a *landing* (zona de pouso: a pasta que
# a ingestão observa).
#
# **Por que importa** — o downloader roda agendado, cai, é reexecutado. Duas propriedades evitam incidente:
# - **idempotente** — arquivo que já está na landing não é baixado de novo (reexecutar não gera trabalho nem cópia);
# - **atômico** — o arquivo aparece na landing **completo ou não aparece**. Se a ingestão listar a pasta no meio
#   de um download, ela leria um gzip pela metade (erro, ou pior: metade dos eventos, sem erro).
#
# **Como funciona** — grava em `.tmp` e renomeia no fim (`rename` é atômico no mesmo sistema de arquivos):

# %%
print(inspect.getsource(gharchive._download))
print(list(gharchive.hour_keys(datetime(2026, 10, 1, 9), datetime(2026, 10, 1, 11))))

# %% [markdown]
# Prova de idempotência, offline: pomos duas horas na landing de DEMO (vindas do `raw_cache`) e pedimos o
# download delas. Nada é baixado — nem há acesso à rede:

# %%
for h in (10, 11):
    shutil.copy2(RAW_CACHE / f"2026-10-01-{h}.json.gz", LANDING)
novos = gharchive.download_hours(datetime(2026, 10, 1, 10), datetime(2026, 10, 1, 11), LANDING)
print("baixados agora:", novos, "| na landing:", sorted(p.name for p in LANDING.iterdir()))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "O download é idempotente — pula o que já existe — e atômico — escreve em `.tmp`
# > e renomeia, então quem observa a pasta nunca vê arquivo pela metade. Com retry exponencial com jitter
# > para falha de rede. Reexecutar o job é sempre seguro."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`rename` atômico depende do storage**: em disco local e no **ADLS Gen2 com namespace hierárquico** é
#   atômico; em Blob Storage "plano" ou S3, renomear é copiar + apagar. Lá o padrão é escrever numa pasta de
#   *staging* fora da observada e mover, ou usar um arquivo de marcação (`_SUCCESS`) que a ingestão espera.
# - **Idempotência por existência** não detecta arquivo corrompido com tamanho > 0; o próximo passo seria
#   conferir checksum/ETag da origem.
# - **Retry com jitter** (`utils/retry.py`): sem jitter, clientes que falharam juntos tentam juntos de novo
#   (*thundering herd*).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Para fontes que **sobrescrevem** o arquivo com o mesmo nome (correção), "já existe" não basta — compare
#   versão/ETag/tamanho.

# %% [markdown]
# ## 2. Landing → bronze incremental: checkpoint e `availableNow`
#
# **O que é** — o *file source* do Structured Streaming trata a pasta como uma fila: cada execução descobre os
# arquivos **novos**, processa, e anota no *checkpoint* (pasta de estado da consulta) o que já leu.
# `trigger(availableNow=True)` = "processe tudo que está disponível agora, em lotes, e pare" — streaming com
# cara de job agendado.
#
# **Por que importa** — é o padrão para ingestão incremental barata: não precisa de serviço rodando 24 h, e o
# custo é proporcional ao **novo**, não ao histórico.
#
# **Como funciona** — `ingest_gharchive_bronze` (notebook 01/`bronze.py`) com landing, alvo e checkpoint de DEMO:
#
# ```text
# landing/*.json.gz ──(lista, compara com o checkpoint)──► arquivos novos ──► Delta bronze (append)
#                                                                 │
#                     checkpoint/offsets  ← "o lote N vai ler isto"   (escrito ANTES de processar)
#                     checkpoint/sources  ← log dos arquivos já vistos
#                     checkpoint/commits  ← "o lote N terminou"       (escrito DEPOIS de gravar)
# ```

# %%
def ingerir(rotulo: str, landing=LANDING, alvo=BRONZE, ckpt=CKPT) -> None:
    q = ingest_gharchive_bronze(spark, str(landing), alvo, ckpt)
    lotes = [p.numInputRows for p in q.recentProgress if p.numInputRows]
    total = spark.read.format("delta").load(alvo).count()
    print(f"{rotulo:<32} lotes com dado={lotes}  linhas na bronze={total:,}")


ingerir("1ª carga (horas 10 e 11)")

# %% [markdown]
# Chega o arquivo da hora 15. Só ele é lido:

# %%
shutil.copy2(RAW_CACHE / "2026-10-01-15.json.gz", LANDING)
ingerir("chegou a hora 15")
ingerir("re-rodar sem arquivo novo")
bronze_demo = spark.read.format("delta").load(BRONZE)
bronze_demo.groupBy(F.regexp_extract("_source_file", r"([^/]+)$", 1).alias("arquivo")).count().orderBy("arquivo").show()

# %% [markdown]
# ### O que o checkpoint guarda (abrindo os arquivos)

# %%
for p in sorted(Path(CKPT).rglob("*")):
    if p.is_file() and not p.name.endswith(".crc"):
        print(rel(p))

# %%
def mostrar(p: Path, n: int = 4) -> None:
    print(f"── {rel(p)}")
    for linha in p.read_text().splitlines()[:n]:
        print("  ", linha[:150])


mostrar(Path(CKPT) / "metadata")
mostrar(Path(CKPT) / "offsets" / "1")
mostrar(Path(CKPT) / "sources" / "0" / "1")
mostrar(Path(CKPT) / "commits" / "1")

# %% [markdown]
# - `metadata` — o id da consulta (estável entre execuções).
# - `offsets/N` — **antes** de processar o lote N: até onde ler (`logOffset` = posição no log de arquivos) e configs.
# - `sources/0/N` — o log do *file source*: cada arquivo descoberto, com o lote em que entrou.
# - `commits/N` — **depois** de gravar: o lote N está concluído.
#
# Se o job cai entre `offsets/N` e `commits/N`, na volta ele **reprocessa o lote N** com exatamente os mesmos
# arquivos. Isso só não duplica porque o *sink* Delta também é idempotente — ele registra no próprio log
# qual (consulta, lote) já gravou:

# %%
for commit in sorted((Path(BRONZE) / "_delta_log").glob("*.json")):
    for linha in commit.read_text().splitlines():
        if '"txn"' in linha:
            print(commit.name, json.loads(linha)["txn"])

# %% [markdown]
# `appId` = id da consulta, `version` = id do lote. Se o lote 1 rodar de novo, o Delta vê que `(appId, 1)` já
# foi commitado e ignora a escrita. **Checkpoint (fonte reproduzível) + sink idempotente = exactly-once**
# (*exatamente uma vez*, de ponta a ponta).
#
# > 🎤 **Resposta de 30 s:** "Ingestão de arquivo incremental: stream com checkpoint e `availableNow`, agendado
# > como job. O checkpoint registra os arquivos vistos e o lote em curso — offset antes, commit depois. Se cair
# > no meio, o lote é refeito com os mesmos arquivos, e o sink Delta ignora o lote repetido pelo `txn` no log.
# > Por isso é exactly-once: fonte reproduzível mais sink idempotente."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`availableNow` × `once`**: `once` processa tudo em **um** lote (pode estourar memória num backlog);
#   `availableNow` respeita `maxFilesPerTrigger`/`maxBytesPerTrigger` e faz vários lotes.
# - **O log do file source cresce** com um registro por arquivo e é compactado a cada N lotes; com milhões de
#   arquivos fica lento — o Auto Loader troca isso por um RocksDB no checkpoint.
# - **Checkpoint é por consulta e não se compartilha**; mudar a lógica de forma incompatível (ex.: agregação
#   com estado) exige checkpoint novo.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Listar uma pasta com centenas de milhares de arquivos a cada execução é lento e caro (chamadas LIST ao
#   storage) — é onde entra o modo *file notification* do Auto Loader (seção 5).

# %% [markdown]
# ## 3. Schema: explícito no envelope, payload como string (ou VARIANT)
#
# **O que é** — o evento tem um **envelope** estável (`id`, `type`, `actor`, `repo`, `org`, `created_at`) e um
# `payload` cujo formato muda conforme o `type` (16 tipos).
#
# **Por que importa** — inferir schema em produção é armadilha: lê dado para adivinhar (custo), adivinha
# diferente a cada lote (instabilidade) e um campo novo muda a tabela em silêncio. E um struct com a união dos
# payloads de 16 tipos teria centenas de colunas quase sempre nulas.
#
# **Como funciona** — envelope com schema explícito (`GH_EVENT_SCHEMA`); `payload` declarado como
# `StringType`: o leitor JSON do Spark devolve o objeto **bruto** como texto. Quem interpreta é a Silver
# (notebook 05). A alternativa moderna é o tipo **VARIANT** (Spark 4 / Delta 4 — roda local):

# %%
print([f"{f.name}: {f.dataType.simpleString()[:40]}" for f in GH_EVENT_SCHEMA.fields])
amostra = spark.read.schema(GH_EVENT_SCHEMA).json(str(PROJECT_ROOT / "tests" / "fixtures" / "gharchive"))
var = amostra.select("type", F.parse_json("payload").alias("payload_v"))
var.filter("type = 'PullRequestEvent'").select(
    "type",
    F.variant_get("payload_v", "$.action", "string").alias("action"),
    F.variant_get("payload_v", "$.pull_request.number", "int").alias("pr_number"),
    F.try_variant_get("payload_v", "$.pull_request.base.ref", "int").alias("ref_como_int"),
).show(3)
var_path = str(DEMO / "payload_variant")
var.write.format("delta").save(var_path)
print("schema gravado no Delta:", spark.read.format("delta").load(var_path).schema.simpleString())

# %% [markdown]
# `try_variant_get` devolve NULL quando o tipo não converte (`ref` não é número) em vez de falhar.
#
# > 🎤 **Resposta de 30 s:** "Envelope com schema explícito, payload como string JSON na bronze — a bronze não
# > interpreta, guarda. Na plataforma nova eu usaria VARIANT: guarda o JSON já em formato binário navegável,
# > leitura por caminho sem reparsear, e o Delta grava o tipo nativamente. Testei aqui no Spark 4.2 + Delta 4.4."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **VARIANT × string JSON**: string reparseia a cada leitura (`from_json`/`get_json_object`); VARIANT é
#   parseado uma vez na escrita e o Databricks ainda faz *shredding* (extrai campos frequentes em colunas
#   para data skipping).
# - **VARIANT × struct**: struct tem tipos fixos e é o mais rápido, mas exige schema conhecido e estável.
# - Leitores antigos (sem a *table feature* `variantType`) não leem a tabela.
# </details>
#
# **Trade-offs** — schema explícito exige manutenção quando a fonte muda (é o ponto: mudança vira decisão).

# %% [markdown]
# ## 4. Registro corrompido e campo novo
#
# **O que é** — o que o leitor faz quando uma linha não casa com o schema: `PERMISSIVE` (padrão: mantém a
# linha, campos ruins viram NULL), `DROPMALFORMED` (descarta a linha) ou `FAILFAST` (aborta).
#
# **Por que importa** — o modo padrão **perde informação em silêncio** se o schema não tiver a coluna de
# registro corrompido. É o comportamento da bronze deste projeto hoje — vale mostrar.
#
# **Como funciona** — um arquivo com 4 linhas: válida, JSON truncado, campo novo (`novo_campo`) e tipo errado
# (`actor.id` texto):

# %%
sujo_dir = DEMO / "landing_suja"
sujo_dir.mkdir()
(sujo_dir / "2026-10-01-99.json").write_text("\n".join([
    '{"id":"1","type":"PushEvent","actor":{"id":7,"login":"ana"},"repo":{"id":1,"name":"a/b"},"payload":{"ref":"main"},"created_at":"2026-10-01T12:00:00Z"}',
    '{"id":"2","type":"PushEvent","actor":{"id":8,"login":"bia"',
    '{"id":"3","type":"WatchEvent","actor":{"id":9,"login":"caio"},"repo":{"id":2,"name":"c/d"},"payload":{"action":"started"},"created_at":"2026-10-01T12:00:01Z","novo_campo":"surpresa"}',
    '{"id":"4","type":"PushEvent","actor":{"id":"dez","login":"duda"},"repo":{"id":3,"name":"e/f"},"payload":{},"created_at":"2026-10-01T12:00:02Z"}',
]) + "\n")

# Como a bronze lê hoje: PERMISSIVE, mas SEM coluna de registro corrompido.
hoje = spark.read.schema(GH_EVENT_SCHEMA).option("mode", "PERMISSIVE").json(str(sujo_dir))
hoje.select("id", "type", "actor.id", "actor.login").show()
print("colunas lidas:", hoje.columns)

# %% [markdown]
# A linha truncada virou uma linha **toda NULL** — entraria na bronze sem nenhum sinal. A de tipo errado manteve
# o `id` e o `login`, mas o `actor.id` virou NULL (só o campo que não converte se perde). E `novo_campo` sumiu
# (schema explícito ignora o que não declara).
#
# Com a coluna `_corrupt_record` no schema, o texto original fica guardado:

# %%
schema_com_corrupt = StructType(GH_EVENT_SCHEMA.fields + [StructField("_corrupt_record", StringType())])
melhor = (spark.read.schema(schema_com_corrupt).option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record").json(str(sujo_dir)).cache())
melhor.select("id", "actor.id", F.substring("_corrupt_record", 1, 60).alias("_corrupt_record")).show(truncate=False)

for modo in ("DROPMALFORMED", "FAILFAST"):
    try:
        lido = spark.read.schema(GH_EVENT_SCHEMA).option("mode", modo).json(str(sujo_dir))
        ids = sorted(r["id"] for r in lido.select("id", F.col("actor.id").alias("actor_id")).collect())
        print(f"{modo:<14} → count() = {lido.count()} · ids ao ler id e actor.id = {ids}")
    except Exception as e:  # noqa: BLE001
        print(f"{modo:<14} → falhou: {type(e).__name__}")

# %% [markdown]
# Repare no `DROPMALFORMED`: o `count()` e a leitura das colunas **não dão o mesmo número**. O Spark só
# interpreta os campos que a consulta pede (*column pruning*, poda de colunas): no `count()` ninguém pede
# `actor.id`, então a linha 4 (tipo errado nesse campo) não é vista como malformada. "Quantas linhas foram
# descartadas" passa a depender da consulta — mais um motivo para não usar esse modo em pipeline.
#
# E as duas opções que **só existem no Databricks** — passadas aqui ao Spark OSS, são ignoradas sem erro:

# %%
bad = DEMO / "bad_records"
r = (spark.read.schema(GH_EVENT_SCHEMA).option("rescuedDataColumn", "_rescued_data")
     .option("badRecordsPath", str(bad)).json(str(sujo_dir)))
print("tem _rescued_data?", "_rescued_data" in r.columns, "| linhas:", r.count(), "| badRecordsPath criado?", bad.exists())

# %% [markdown]
# No Databricks ☁️:
# - `rescuedDataColumn` → coluna `_rescued_data` (JSON) com **o que não coube no schema**: campo novo
#   (`{"novo_campo":"surpresa"}`), tipo divergente (`{"id":"dez"}` dentro de `actor`). Nada se perde e a linha
#   continua utilizável.
# - `badRecordsPath` → linhas/arquivos ilegíveis são gravados num caminho de exceções, com o motivo.
#
# > 🎤 **Resposta de 30 s:** "Com schema explícito, o risco é perder dado calado: PERMISSIVE sem coluna de
# > corrupção transforma JSON quebrado em linha nula, e campo novo some. Localmente eu ponho `_corrupt_record`
# > no schema e monitoro. No Databricks, Auto Loader com `rescuedDataColumn` guarda tudo que não casou, e
# > `badRecordsPath` separa o ilegível."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Correção para a bronze daqui**: incluir `_corrupt_record` no `GH_EVENT_SCHEMA` (ou na leitura) e uma
#   expectation `_corrupt_record IS NULL` com ação warn/quarentena (notebook 08).
# - Spark não deixa consultar **só** a coluna `_corrupt_record` direto do arquivo cru (erro
#   `UNSUPPORTED_FEATURE`/corrupt record only) — por isso o `.cache()` antes.
# - `FAILFAST` é bom em desenvolvimento e em fontes contratadas; em ingestão de terceiros, prefira capturar e
#   medir.
# </details>
#
# **Trade-offs** — `DROPMALFORMED` "resolve" sem deixar rastro; só aceitável com métrica de quantas caíram.

# %% [markdown]
# ## 5. Auto Loader no Databricks ☁️
#
# **O que é** — o *source* `cloudFiles`: o mesmo modelo (stream + checkpoint + `availableNow`), com
# descoberta de arquivos escalável, inferência e evolução de schema gerenciadas, e dado resgatado.
#
# **Por que importa** — é a troca de **uma linha** (`format("cloudFiles")`) que leva este pipeline para
# produção com milhões de arquivos.
#
# **Como funciona** — snippet completo:
#
# ```python
# bronze = (
#     spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", "abfss://lake@<conta>.dfs.core.windows.net/_schemas/gh_events")
#     .option("cloudFiles.inferColumnTypes", "true")          # sem isso, JSON inferido vira tudo string
#     .option("cloudFiles.schemaHints", "payload STRING, id STRING, created_at STRING")  # fixa o que importa
#     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # padrão: coluna nova → para, atualiza schema, reinicia
#     .option("rescuedDataColumn", "_rescued_data")           # o que não couber vai para cá
#     .option("cloudFiles.maxFilesPerTrigger", 100)
#     .load("abfss://lake@<conta>.dfs.core.windows.net/landing/gharchive/")
#     .select("*", "_metadata.file_path", "_metadata.file_modification_time")
# )
# (bronze.writeStream
#     .option("checkpointLocation", "abfss://lake@<conta>.dfs.core.windows.net/_checkpoints/bronze_gh_events")
#     .trigger(availableNow=True)
#     .toTable("oss.bronze.gh_events"))
# ```
#
# `cloudFiles.schemaLocation` guarda o schema inferido **versionado** (`_schemas/0`, `1`, …); a cada coluna nova,
# uma versão nova. Modos de `schemaEvolutionMode`:
#
# | Modo | Coluna nova na fonte |
# |---|---|
# | `addNewColumns` (padrão sem schema fixo) | o stream **falha** de propósito, grava o schema novo; ao reiniciar (Lakeflow Job com retry) segue com a coluna |
# | `rescue` | schema não muda; a coluna nova vai para `_rescued_data` |
# | `failOnNewColumns` | falha e **não** evolui — exige ação humana |
# | `none` | ignora a coluna nova (perde o dado se não houver `rescuedDataColumn`) |
#
# **Descoberta de arquivos — directory listing × file notification:**
#
# | | Directory listing (padrão) | File notification |
# |---|---|---|
# | Como descobre | LIST incremental da pasta a cada lote | evento do storage → fila → Auto Loader consome |
# | Azure | chamadas LIST no ADLS Gen2 | **Event Grid** assina "BlobCreated" e entrega numa **fila do Azure Storage (Queue)** |
# | Escala | ok até ~milhares de arquivos novos por lote | milhões de arquivos, latência baixa |
# | Configuração | nenhuma | `cloudFiles.useNotifications=true` + permissão para criar Event Grid/Queue (ou recursos pré-criados) |
#
# Recente: com **file events** habilitados na *external location* do Unity Catalog, o Auto Loader usa
# notificações gerenciadas pelo Databricks (`cloudFiles.useManagedFileEvents`) sem cada stream criar a sua fila.
#
# > 🎤 **Resposta de 30 s:** "Auto Loader é o `cloudFiles`: mesmo checkpoint e `availableNow`, mas com RocksDB
# > guardando os arquivos vistos, schema inferido e versionado no `schemaLocation`, evolução por
# > `schemaEvolutionMode` e `rescuedDataColumn` para não perder nada. Para muitos arquivos, troco directory
# > listing por file notification — na Azure, Event Grid entregando numa fila do Storage."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Notificação perde evento?** Pode atrasar/perder raramente; `cloudFiles.backfillInterval` (ex.: `1 day`)
#   faz um listing periódico de segurança.
# - **Ordem**: nem listing nem notificação garantem ordem de chegada — a Silver não pode depender dela (notebook 05).
# - **`_metadata`**: `file_path`, `file_modification_time`, `file_size` — linhagem sem custo.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Fonte que **reescreve** arquivos com o mesmo nome: por padrão o Auto Loader não relê (`cloudFiles.allowOverwrites` existe, com cuidado).
# - Poucos arquivos grandes por dia, sem urgência: `COPY INTO` ou batch simples resolve.

# %% [markdown]
# ## 6. Backfill e reprocessamento
#
# **O que é** — *backfill*: carregar um período passado (fonte nova com histórico, correção de bug,
# arquivo que chegou atrasado).
#
# **Por que importa** — é onde o exactly-once **acaba**: ele vale enquanto o checkpoint existir. Apagar o
# checkpoint para "reprocessar tudo" sobre o mesmo alvo em modo append **duplica**. Prova (com a amostra de
# 2.000 eventos, numa bronze de DEMO separada):

# %%
fixture = PROJECT_ROOT / "tests" / "fixtures" / "gharchive"
alvo_bf = str(DEMO / "bronze_backfill")
ingerir("carga normal", landing=fixture, alvo=alvo_bf, ckpt=str(DEMO / "_ck_bf_1"))
ingerir("checkpoint novo, mesmo alvo", landing=fixture, alvo=alvo_bf, ckpt=str(DEMO / "_ck_bf_2"))
dups = spark.read.format("delta").load(alvo_bf).groupBy("id").count().filter("count > 1").count()
print("ids duplicados:", f"{dups:,}")

# %% [markdown]
# Estratégias, da mais simples à mais robusta:
#
# 1. **Bronze tolera duplicata, Silver resolve** — este projeto: a bronze é *at-least-once* e a Silver faz MERGE
#    por `event_id` (notebook 05). Reprocessar a bronze não contamina a Silver.
# 2. **Backfill de um intervalo com sobrescrita seletiva** — job batch separado que regrava só o intervalo:
#    ```python
#    (spark.read.schema(GH_EVENT_SCHEMA).json(".../landing/gharchive/2026-10-01-*.json.gz")
#        .transform(add_ingestion_metadata)
#        .write.format("delta").mode("overwrite")
#        .option("replaceWhere", "event_date = '2026-10-01'")   # atômico: troca só aquela data
#        .save(BRONZE))
#    ```
# 3. **Auto Loader ☁️**: checkpoint novo + `cloudFiles.includeExistingFiles=true` (padrão) para reler tudo;
#    `modifiedAfter`/`modifiedBefore` para recortar por data de modificação; `cloudFiles.backfillInterval` para
#    pegar arquivos que a notificação perdeu.
#
# > 🎤 **Resposta de 30 s:** "Exactly-once vale enquanto o checkpoint existe. Para backfill eu não confio em
# > apagar checkpoint: ou a camada seguinte é idempotente (MERGE por chave), ou faço um batch que regrava só o
# > intervalo com `replaceWhere`, que é atômico. Mostrei aqui: checkpoint novo no mesmo alvo duplicou tudo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Backfill grande junto com a carga do dia**: rode o backfill como stream separado (checkpoint e
#   `maxFilesPerTrigger` próprios) para não atrasar o dado novo; o destino idempotente aceita os dois.
# - **`replaceWhere` valida o dado**: se o DataFrame trouxer linha fora do predicado, o Delta recusa a escrita
#   — proteção contra sobrescrever a data errada.
# - **Reprocessar por bug de transformação** não exige reler a fonte: a bronze guarda o dado cru; refaz-se só
#   da bronze para a frente.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `replaceWhere` troca o intervalo inteiro: se só parte dos arquivos do dia estiver disponível, você
#   **apaga** o que já tinha. Confira a completude da fonte antes.
# - Bronze *at-least-once* + Silver idempotente custa um MERGE; para volume extremo, backfill particionado
#   com sobrescrita costuma sair mais barato.

# %% [markdown]
# ## 7. `COPY INTO` × Auto Loader ☁️
#
# **O que é** — `COPY INTO` é o comando SQL do Databricks para carregar arquivos numa tabela Delta, também
# idempotente por arquivo. **Por que importa** — é a pergunta clássica "qual dos dois?"; a resposta depende de
# volume de arquivos, evolução de schema e de quem opera (time de SQL × engenharia).
#
# ```sql
# COPY INTO oss.bronze.gh_events
# FROM (SELECT *, _metadata.file_path AS _source_file FROM 'abfss://lake@<conta>.dfs.core.windows.net/landing/gharchive/')
# FILEFORMAT = JSON
# FORMAT_OPTIONS ('inferSchema' = 'false')
# COPY_OPTIONS ('mergeSchema' = 'true');
# ```
#
# | | `COPY INTO` | Auto Loader |
# |---|---|---|
# | Interface | SQL, comando batch | stream (`cloudFiles`), batch via `availableNow` |
# | Estado dos arquivos vistos | no log da tabela-alvo | no checkpoint (RocksDB) |
# | Escala | milhares de arquivos | milhões; file notification |
# | Schema | `mergeSchema` | inferência, `schemaLocation`, `schemaEvolutionMode`, rescued data |
# | Quando usar | carga SQL simples, poucos arquivos, time de SQL | ingestão contínua/incremental de produção, Lakeflow |
#
# Ambos são idempotentes por arquivo: rodar de novo não recarrega o que já entrou.
#
# > 🎤 **Resposta de 30 s:** "Os dois carregam cada arquivo uma vez. `COPY INTO` é SQL, simples, bom para
# > milhares de arquivos e carga pontual. Auto Loader é stream com checkpoint, escala para milhões com file
# > notification e trata evolução de schema — é o padrão para ingestão contínua. Em pipeline novo no
# > Databricks eu começo pelo Auto Loader (ou `read_files` numa streaming table)."
#
# **Trade-offs** — `COPY INTO` guarda o estado na própria tabela (recriou a tabela, recarrega tudo) e fica
# lento para listar diretórios enormes; Auto Loader exige cuidar do checkpoint e do `schemaLocation`.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# - **Auto Loader** em **Lakeflow Declarative Pipelines**: `CREATE OR REFRESH STREAMING TABLE bronze AS SELECT * FROM STREAM read_files('abfss://…/landing/gharchive/', format => 'json')` — checkpoint e schemaLocation gerenciados.
# - **Lakeflow Jobs** agendam o `availableNow`; com **file arrival trigger** o job dispara quando chega arquivo
#   novo na external location (sem cron).
# - **ADLS Gen2** com namespace hierárquico (rename atômico), acesso pelo **Access Connector** (identidade
#   gerenciada) registrado como *storage credential* no Unity Catalog — sem chave de conta no código.
# - **Event Grid + Queue Storage** para file notification (ou file events gerenciados na external location).
# - **Lakeflow Connect**: conectores gerenciados (SQL Server, Salesforce, SharePoint…) quando a fonte não é arquivo.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Como você garante que um arquivo é processado exatamente uma vez?**
# <details><summary>Resposta</summary>
# Checkpoint (offsets antes, commits depois, log dos arquivos vistos) + sink idempotente (Delta registra
# `txn(appId, version)` e ignora lote repetido). Mostrado abrindo os arquivos.
# </details>
#
# **2. `trigger(availableNow=True)` × `once` × contínuo?**
# <details><summary>Resposta</summary>
# availableNow processa o backlog em vários lotes (respeita maxFilesPerTrigger) e para — ideal para job
# agendado; once faz um lote só (risco de memória); processingTime/contínuo mantém o cluster ligado.
# </details>
#
# **3. Por que não inferir o schema?**
# <details><summary>Resposta</summary>
# Custo de leitura, instabilidade entre lotes, mudança silenciosa. Envelope explícito, payload bruto (string ou
# VARIANT) e evolução controlada (`schemaEvolutionMode`, rescued data).
# </details>
#
# **4. O que acontece com uma linha JSON corrompida no modo padrão?**
# <details><summary>Resposta</summary>
# PERMISSIVE: sem coluna de corrupção, vira linha toda nula (perda silenciosa — demonstrado). Com
# `_corrupt_record`, o texto fica guardado; no Databricks, `badRecordsPath` e `rescuedDataColumn`.
# </details>
#
# **5. A fonte adicionou um campo. O que acontece no seu pipeline?**
# <details><summary>Resposta</summary>
# Local com schema explícito: o campo é ignorado. Auto Loader: `addNewColumns` para o stream, evolui o schema e
# segue no restart; `rescue` manda para `_rescued_data`; `failOnNewColumns` exige ação.
# </details>
#
# **6. Directory listing ou file notification?**
# <details><summary>Resposta</summary>
# Listing é simples e serve até milhares de arquivos por lote; notification (Event Grid + Queue na Azure) escala
# para milhões e reduz latência/custo de LIST. Com backfillInterval de segurança.
# </details>
#
# **7. Como você faz backfill sem duplicar?**
# <details><summary>Resposta</summary>
# Não apagando checkpoint sobre alvo append (duplica — demonstrado). Camada seguinte idempotente (MERGE) ou batch
# com `replaceWhere` no intervalo.
# </details>
#
# **8. COPY INTO ou Auto Loader?**
# <details><summary>Resposta</summary>
# COPY INTO para carga SQL simples e poucos arquivos; Auto Loader para ingestão incremental de produção, muitos
# arquivos, evolução de schema e Lakeflow.
# </details>
#
# **9. Por que o download grava em `.tmp` e renomeia?**
# <details><summary>Resposta</summary>
# Para a ingestão nunca ver arquivo parcial; rename é atômico no mesmo filesystem e no ADLS Gen2 com HNS. Em
# object store sem rename atômico: pasta de staging ou marcador `_SUCCESS`.
# </details>

# %% [markdown]
# ## Resumo
#
# - Download idempotente (pula o que existe) e atômico (`.tmp` + rename); retry com jitter.
# - Stream de arquivos + checkpoint + `availableNow`: só o arquivo novo é lido; re-rodar não duplica — offsets/sources/commits + `txn` no log Delta = exactly-once.
# - Exactly-once morre com o checkpoint: reprocessar com checkpoint novo em append duplica → idempotência na camada seguinte ou `replaceWhere`.
# - Schema explícito no envelope, payload string/VARIANT; PERMISSIVE sem `_corrupt_record` perde linha em silêncio.
# - Auto Loader ☁️: `schemaLocation`, `schemaEvolutionMode`, `rescuedDataColumn`, file notification (Event Grid + Queue).

# %%
spark.stop()
