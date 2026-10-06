# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 06 · Streaming com Structured Streaming (Wikimedia EventStreams)
#
# > Edições de todas as wikis do mundo, em tempo real, viram contagens por minuto com **tempo de evento**,
# > **watermark** que descarta dado atrasado, **MERGE idempotente** por micro-lote e garantia **exactly-once**
# > — tudo provado com as métricas do próprio Spark.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Databricks e processamento de dados | Structured Streaming, janelas, watermark, state store (RocksDB), triggers |
# | Arquitetura e desenvolvimento de pipelines | landing em micro-lotes, checkpoint, exactly-once, `foreachBatch` + MERGE |
# | Python avançado | consumidor SSE com geradores, escrita atômica, retomada por `Last-Event-ID` |
# | Microsoft Azure | Event Hubs pelo endpoint Kafka ☁️, Auto Loader ☁️ |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Dados.** Por padrão o notebook usa uma captura **real** de ~4 minutos do stream `recentchange`, gravada em
# 2026-10-05 em `tests/fixtures/wikimedia/` (12 micro-lotes `.jsonl.gz`, campos recortados). A captura ao vivo é
# opcional: `OSSLH_WIKI_LIVE=1`.

# %% [markdown]
# ## Setup

# %%
import gzip
import json
import os
import shutil
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.sources import wikimedia as wm
from oss_lakehouse.spark import get_spark
from oss_lakehouse.streaming import (
    WIKI_SCHEMA,
    dedup_events,
    edits_per_window,
    merge_batch,
    parse_wiki,
    progress_summary,
    read_wiki_stream,
)

s = get_settings()
spark = get_spark("06")
FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "wikimedia" / "recentchange"
FILES = sorted(FIXTURE.glob("*.jsonl.gz"))
DEMO = Path(s.data_root) / "demo" / "06"  # tudo que é experimento recomeça do zero
shutil.rmtree(DEMO, ignore_errors=True)
LAND = DEMO / "landing"
LAND.mkdir(parents=True)


def arrive(files, start_mtime=1_000_000):
    """Simula a chegada de arquivos na landing (o file source processa por data de modificação)."""
    for i, f in enumerate(files):
        dst = LAND / f.name
        shutil.copy2(f, dst)
        os.utime(dst, (start_mtime + i, start_mtime + i))


def run(query):
    """availableNow: processa o que há e para. Devolve só os progressos com dado."""
    query.awaitTermination()
    return [p for p in query.recentProgress if p["numInputRows"] > 0]


n_events = sum(1 for f in FILES for _ in gzip.open(f, "rt"))
print(f"fixture: {len(FILES)} arquivos, {n_events} eventos")

# %% [markdown]
# ## 1. A fonte: SSE com retomada por `Last-Event-ID`
#
# **O que é.** **SSE** (*Server-Sent Events*) é HTTP comum cuja resposta nunca termina: o servidor manda blocos
# `event:` / `id:` / `data:` separados por linha em branco. O EventStreams da Wikimedia expõe assim um tópico Kafka
# interno: o `id` de cada evento é a posição (partição/offset ou timestamp) nesse Kafka.
#
# **Por que importa.** Conexão longa cai — rede, deploy do servidor, timeout de proxy. Quem reconecta mandando
# o header `Last-Event-ID` com o último `id` recebido continua de onde parou, sem buraco. É o mesmo conceito de
# **offset** de consumidor Kafka.
#
# **Como funciona.** `wikimedia.capture` lê o stream e grava **micro-lotes** JSONL.gz na landing (fecha o arquivo
# a cada N eventos ou T segundos), com escrita atômica (`.tmp` → rename). O último `id` vai para um arquivo de estado
# **depois** de o lote estar em disco (dado → estado). Se o processo morrer, ele reprocessa no máximo um lote:
# **at-least-once** — e a deduplicação por `meta.id` fecha a conta (seção 4).
#
# ```text
# stream.wikimedia.org ──SSE──▶ capture() ──a cada 20 s──▶ landing/wikimedia/rc-<ts>-NNNNN.jsonl.gz
#        ▲                          │
#        └── Last-Event-ID ◀── state.json (último id gravado)
# ```

# %%
first = json.loads(gzip.open(FILES[0], "rt").readline())
sse = ["event: message", 'id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":42}]',
       "data: " + json.dumps(first, ensure_ascii=False), "", ":keep-alive", ""]
ev = next(wm.parse_sse(sse))
print("id (o que vai no Last-Event-ID):", ev.id)
print("evento:", json.dumps(json.loads(ev.data), ensure_ascii=False)[:230], "…")
print("User-Agent enviado:", wm.USER_AGENT)

# %%
LIVE_DIR = Path(s.path("landing", "wikimedia"))
if os.environ.get("OSSLH_WIKI_LIVE") == "1":  # 🧪 opcional: precisa de internet
    try:
        rep = wm.capture(LIVE_DIR, LIVE_DIR / "_state" / "last_event_id.json", max_seconds=30, batch_seconds=10)
        print(f"ao vivo: {rep.events} eventos em {len(rep.files)} arquivos; retomou de: {rep.resumed_from}")
    except Exception as exc:  # sem rede: segue com a fixture
        print(f"captura ao vivo falhou ({type(exc).__name__}); seguindo com a fixture")
else:
    print("captura ao vivo desligada (OSSLH_WIKI_LIVE=1 liga); usando a fixture gravada")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "O consumidor grava micro-lotes atômicos na landing e só depois persiste o último id.
# > Se cair, reconecta com `Last-Event-ID` e no pior caso relê um lote — at-least-once na ingestão. A duplicata
# > morre na Silver, por chave do evento. Isso separa *receber* de *processar*: o Spark lê arquivos, que são
# > replayable."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que não ler o SSE direto no Spark?** Não há fonte SSE nativa, e um socket não é *replayable*: se o job
#   falhar, o dado do meio se perde. Landing em arquivo (ou Kafka/Event Hubs) dá fonte reexecutável — pré-requisito
#   de exactly-once.
# - O EventStreams também aceita `?since=<timestamp>` para recomeçar de um instante (retenção limitada).
# - A Wikimedia exige `User-Agent` descritivo com contato; cliente genérico é bloqueado.
# - O consumidor descarta o domínio `canary` (eventos sintéticos de monitoramento da própria Wikimedia).
# </details>
#
# **Trade-offs**
# - Micro-lote de 20 s = latência mínima de ~20 s até a landing; menor que isso gera *small files* (notebook 09).
# - Um processo Python consumindo é ponto único de falha; em produção, o produtor publica em Kafka/Event Hubs ☁️.

# %% [markdown]
# ## 2. Schema explícito e o file source como stream
#
# **O que é.** **Structured Streaming** trata o stream como uma tabela que só cresce: você escreve a consulta como
# se fosse batch (DataFrame) e o Spark a executa **incrementalmente**, em micro-lotes, guardando no **checkpoint**
# o que já processou.
#
# **Por que importa.** O mesmo código de transformação serve para batch e streaming; e o checkpoint transforma
# "processar o que chegou desde a última vez" em problema resolvido.
#
# **Como funciona.** `readStream.schema(WIKI_SCHEMA).json(landing)` — schema **obrigatoriamente explícito** em
# stream de arquivo (inferir exigiria ler tudo a cada início, e o schema mudaria sozinho). `maxFilesPerTrigger`
# define o tamanho do micro-lote; `pathGlobFilter` ignora o `.tmp` do consumidor.

# %%
arrive(FILES)
stream = parse_wiki(read_wiki_stream(spark, str(LAND), max_files_per_trigger=3))
print("é stream?", stream.isStreaming)
stream.printSchema()

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Structured Streaming é batch incremental: a consulta é um DataFrame comum, o Spark
# > roda em micro-lotes e o checkpoint guarda offsets e estado. Em stream de arquivo o schema é sempre explícito
# > — inferência em produção é como deixar o produtor mudar seu contrato sem avisar."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - O file source lista o diretório a cada micro-lote e guarda no checkpoint (`sources/0/`) quais arquivos já leu.
#   Com milhões de arquivos a listagem fica cara — é exatamente o problema que o **Auto Loader** ☁️ resolve
#   (RocksDB interno de arquivos vistos + notificação de eventos do storage).
# - Arquivo **modificado** depois de lido não é relido: a landing precisa ser imutável (por isso o rename atômico).
# - `_metadata.file_path` dá a linhagem por linha sem custo extra.
# </details>
#
# **Trade-offs**
# - `maxFilesPerTrigger` pequeno = latência baixa e muitos commits pequenos; grande = lotes eficientes e mais
#   latência. Para fontes com bytes irregulares, `maxBytesPerTrigger` (Auto Loader) controla melhor.

# %% [markdown]
# ## 3. Tempo de evento × tempo de processamento
#
# **O que é.** **Event time** é quando o fato aconteceu (o `meta.dt` do evento); **processing time** é quando o
# cluster processou (`_ingested_at`). Agregação de negócio ("edições por minuto") tem de usar event time.
#
# **Por que importa.** Atraso de rede, reprocessamento e backfill afastam os dois relógios. Contar por processing
# time faz o resultado depender de **quando** o job rodou — rodar de novo dá outro número.
#
# **Como funciona.** Com a mesma amostra lida em batch (o código é o mesmo do stream), comparamos as duas contagens
# por minuto. Esta execução acontece bem depois da captura (reprocessamento): o caso extremo de atraso.

# %%
static = parse_wiki(spark.read.schema(WIKI_SCHEMA).option("pathGlobFilter", "*.jsonl.gz")
                    .json(str(LAND)).select("*", "_metadata"))
lag = static.select(((F.col("_ingested_at").cast("double") - F.col("event_time").cast("double")) / 3600)
                    .alias("h"))
print(f"atraso processamento − evento: {lag.agg(F.min('h')).first()[0]:.1f} h a {lag.agg(F.max('h')).first()[0]:.1f} h")
by_event = static.groupBy(F.window("event_time", "1 minute").start.alias("minuto")).count()
by_proc = static.groupBy(F.window("_ingested_at", "1 minute").start.alias("minuto")).count()
print("janelas de 1 min por tempo de EVENTO:")
by_event.orderBy("minuto").show(truncate=False)
print("janelas por tempo de PROCESSAMENTO:", by_proc.count(), "(tudo cai no minuto em que o job rodou)")

# %% [markdown]
# Um detalhe de relógio: os arquivos têm no nome o carimbo de quando **esta máquina** os fechou; os eventos têm o
# `meta.dt` do **servidor** da Wikimedia. Comparando os dois:

# %%
stamps = []
for f in FILES:
    closed = time.mktime(time.strptime(f.name[3:18], "%Y%m%dT%H%M%S"))  # relógio local (UTC no nome)
    evts = [json.loads(x)["meta"]["dt"] for x in gzip.open(f, "rt")]
    newest = time.mktime(time.strptime(max(evts)[:19], "%Y-%m-%dT%H:%M:%S"))
    stamps.append(closed - newest)
print(f"arquivo fechado − evento mais novo dele: de {min(stamps):.0f} s a {max(stamps):.0f} s")

# %% [markdown]
# Valores negativos significam arquivo "fechado antes" do evento que ele contém — impossível, a não ser que os
# relógios discordem: o desta máquina está cerca de 10 s atrás do servidor. Relógios divergem sempre; é mais um
# motivo para medir tempo pelo evento e dar folga (watermark) em vez de confiar no relógio do cluster.
#
# > 🎤 **Resposta de 30 s:** "Agrego por tempo de evento, nunca de processamento: o resultado tem de ser o mesmo
# > se eu reprocessar amanhã. Processing time só serve para métricas operacionais — latência, throughput."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Há um terceiro tempo: **ingestion time** (quando chegou à landing/Kafka). Kafka carimba `timestamp` na
#   mensagem — útil quando o evento não traz o seu.
# - Fuso: event time em UTC, sempre; a sessão aqui usa `spark.sql.session.timeZone=UTC`. Atenção: `collect()` no
#   Python converte para o fuso da máquina — formate no Spark quando precisar de texto.
# </details>
#
# **Trade-offs**
# - Event time exige confiar no relógio do produtor; evento com relógio absurdo (ano 1970) precisa de regra de
#   qualidade (notebook 08).

# %% [markdown]
# ## 4. Checkpoint, sink Delta e exactly-once
#
# **O que é.** **Exactly-once** (efeito exatamente uma vez) = cada evento afeta o resultado uma única vez, mesmo
# com falhas e reexecuções. Em Structured Streaming isso sai de três peças: **fonte replayable** (arquivo, Kafka:
# dá para reler um intervalo), **checkpoint** (offsets e estado gravados por micro-lote) e **sink idempotente**
# (reescrever o mesmo lote não duplica).
#
# **Por que importa.** Falha é rotina: nó perdido, deploy, *spot* reclamado. Sem as três peças, toda falha vira
# duplicata ou buraco, e alguém reconcilia na mão.
#
# **Como funciona.** O sink Delta grava, no mesmo commit dos dados, uma ação `txn` com `(appId = id da consulta,
# version = batchId)`. Se o Spark reexecutar um lote já commitado, o Delta reconhece a versão e não grava de novo.
# Abaixo: chegam 6 arquivos, roda; chegam mais 6, roda; roda de novo sem nada novo. Deduplicação por `event_id`
# (`dropDuplicatesWithinWatermark`) absorve a reentrega at-least-once do consumidor.

# %%
shutil.rmtree(LAND)
LAND.mkdir()
BRONZE_WIKI, CK_BRONZE = str(DEMO / "wiki_changes"), str(DEMO / "_ck" / "wiki_changes")


def to_delta():
    events = dedup_events(parse_wiki(read_wiki_stream(spark, str(LAND), max_files_per_trigger=3)))
    return (events.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", CK_BRONZE).trigger(availableNow=True).start(BRONZE_WIKI))


for label, files in [("chegam 6 arquivos", FILES[:6]), ("chegam mais 6", FILES[6:]), ("nada novo", [])]:
    arrive(files, start_mtime=1_000_000 + len(list(LAND.iterdir())))
    progress = run(to_delta())
    total = spark.read.format("delta").load(BRONZE_WIKI).count()
    print(f"{label:18s} → micro-lotes com dado: {len(progress)}, linhas lidas: "
          f"{sum(p['numInputRows'] for p in progress):5d}, total na tabela: {total}")

# %%
log_dir = Path(BRONZE_WIKI) / "_delta_log"
txns = [json.loads(line)["txn"] for f in sorted(log_dir.glob("*.json"))
        for line in f.read_text().splitlines() if '"txn"' in line]
print("ações txn no log do Delta (uma por micro-lote):")
for t in txns[:3]:
    print("  ", {k: t[k] for k in ("appId", "version")})
print("   …", len(txns), "no total; arquivos do checkpoint:", sorted(p.name for p in Path(CK_BRONZE).iterdir()))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Exactly-once em Structured Streaming é fonte replayable mais checkpoint mais sink
# > idempotente. O checkpoint registra o intervalo de cada micro-lote antes de processar e o commit depois; se cair
# > no meio, o Spark refaz o mesmo lote, e o Delta descarta a regravação porque guarda `(queryId, batchId)` no log."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Pastas do checkpoint: `offsets/` (o que o lote N vai ler — escrito **antes**), `commits/` (lote N terminou —
#   escrito **depois**), `sources/` (arquivos já vistos), `state/` (estado de agregação/dedupe), `metadata` (id da
#   consulta).
# - **Um checkpoint por consulta, nunca compartilhado**; e mudar a consulta de forma incompatível (ex.: chave de
#   agregação) exige checkpoint novo.
# - A garantia é *end-to-end* só se o sink for idempotente. Sink "manda e-mail"/"chama API" é at-least-once —
#   precisa de chave de idempotência no destino.
# - `dropDuplicatesWithinWatermark` guarda cada id só durante o watermark; `dropDuplicates` sem watermark guarda
#   para sempre (estado infinito).
# </details>
#
# **Trade-offs**
# - Checkpoint é estado de produção: apagar = reprocessar tudo (ou perder a posição). Versione o caminho junto com o
#   código.
# - Commit por micro-lote em Delta gera arquivos pequenos: `OPTIMIZE`/auto compaction resolvem (notebook 09).

# %% [markdown]
# ## 5. Janelas tumbling e sliding, e os três output modes
#
# **O que é.**
# - **Janela tumbling** (*em degraus*): fixa e sem sobreposição — 13:24–13:25, 13:25–13:26… cada evento cai em uma.
# - **Janela sliding** (*deslizante*): tamanho 2 min andando de 1 em 1 min — cada evento cai em **duas** janelas.
# - **Output mode**: o que o sink recebe a cada micro-lote. `append` = só linhas **finais** (janela fechada pelo
#   watermark); `update` = só as linhas que **mudaram** neste lote; `complete` = a tabela de resultado **inteira**.
#
# **Por que importa.** O output mode define a semântica do destino: `append` serve a arquivo/Delta append (linha
# escrita nunca muda); `update` serve a MERGE; `complete` só cabe em resultado pequeno (é reescrito todo lote).
#
# **Como funciona.** A mesma agregação — edições por minuto, por wiki e bot × humano, watermark de 1 min — rodando
# nos três modos sobre a captura (4 micro-lotes de 3 arquivos). O `foreachBatch` só conta o que chegou ao sink.

# %%
def per_batch_rows(mode, window="1 minute", slide=None):
    seen = []
    agg = edits_per_window(parse_wiki(read_wiki_stream(spark, str(LAND), max_files_per_trigger=3)),
                           window, slide, watermark="1 minute")
    q = (agg.writeStream.outputMode(mode)
         .foreachBatch(lambda df, bid: seen.append((bid, df.count(), df.select("window_start").distinct().count())))
         .option("checkpointLocation", str(DEMO / "_ck" / f"modes_{mode}_{slide}")).trigger(availableNow=True).start())
    q.awaitTermination()
    return seen


for mode in ("append", "update", "complete"):
    rows = per_batch_rows(mode)
    print(f"{mode:9s} (batchId, linhas, janelas distintas) → {rows}")

# %% [markdown]
# Leitura: em `complete` o sink recebe a tabela inteira a cada lote (cresce sempre); em `update`, só as janelas
# tocadas no lote; em `append`, só janelas cujo fim ficou para trás do watermark — e as **últimas janelas nunca
# saem** nesta execução (das 5 janelas da captura, só 3 foram emitidas em append): com `availableNow`, o stream
# acaba e o watermark não avança além do último evento. Elas ficam no estado esperando dado novo. É a pegadinha
# clássica de append + janela.
#
# Agora o resultado de negócio (tumbling), e a prova de que a sliding conta cada evento duas vezes:

# %%
events_static = static.where("event_time IS NOT NULL")
tumbling = events_static.groupBy(F.window("event_time", "1 minute").alias("w"), "is_bot").count()
(tumbling.groupBy(F.date_format("w.start", "HH:mm").alias("minuto"))
 .pivot("is_bot", [False, True]).sum("count").withColumnRenamed("false", "humanos")
 .withColumnRenamed("true", "bots").orderBy("minuto").show())
sliding = events_static.groupBy(F.window("event_time", "2 minutes", "1 minute")).count()
print(f"eventos: {events_static.count()} | soma tumbling: {tumbling.agg(F.sum('count')).first()[0]} "
      f"| soma sliding(2 min, passo 1 min): {sliding.agg(F.sum('count')).first()[0]}")
(events_static.groupBy("wiki").agg(F.count("*").alias("edicoes"), F.avg(F.col("is_bot").cast("int")).alias("pct_bot"))
 .orderBy(F.desc("edicoes")).withColumn("pct_bot", F.round(F.col("pct_bot") * 100, 1)).show(5))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Tumbling para métricas por período, sliding para média móvel — cada evento cai em
# > tamanho/passo janelas. O output mode casa com o sink: append para tabela imutável, depois que o watermark fecha a
# > janela; update para MERGE; complete só para resultado pequeno, tipo dashboard."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Session window** (`F.session_window`): janela que fecha após X minutos de inatividade por chave — sessões de
#   usuário.
# - `append` exige watermark em agregação (senão nenhuma linha seria final); `complete` não remove estado nunca.
# - Para não ficar com a última janela presa, em produção o stream é contínuo (chega dado novo) ou usa-se `update`
#   + MERGE (seção 7), que mostra o valor parcial e o corrige depois.
# </details>
#
# **Trade-offs**
# - Sliding com passo pequeno multiplica estado e saída (2 min / 10 s = 12 janelas por evento).
# - `update` no destino exige MERGE (mais caro que append) — mas é o que dá resultado parcial cedo.

# %% [markdown]
# ## 6. Watermark: quanto esperar pelo atrasado — e descartar o resto
#
# **O que é.** **Watermark** = "máximo tempo de evento visto − atraso tolerado". Evento mais velho que o watermark
# é **descartado**; janela cujo fim ficou para trás dele é **finalizada** e o estado dela é liberado.
#
# **Por que importa.** Sem watermark, uma agregação por janela guarda estado de todas as janelas para sempre (o
# Spark não sabe se ainda vem dado de 3 dias atrás): memória cresce até o job cair. O watermark troca
# **completude** por **memória limitada**, com uma regra explícita.
#
# **Como funciona.** Rodamos a agregação em `update` com MERGE no Delta (a seção 7 explica o MERGE). Depois
# injetamos um arquivo com dois eventos: um de **4 minutos atrás** do último visto (atrás do watermark) e outro de
# **30 s atrás** (dentro da tolerância de 1 min).

# %%
AGG, CK_AGG = str(DEMO / "edits_per_minute"), str(DEMO / "_ck" / "edits_per_minute")
KEYS = ["window_start", "wiki", "is_bot"]
merges = []
sink = merge_batch(AGG, KEYS, on_batch=lambda bid, n: merges.append((bid, n)))


def agg_query():
    agg = edits_per_window(parse_wiki(read_wiki_stream(spark, str(LAND), max_files_per_trigger=3)),
                           "1 minute", watermark="1 minute")
    return (agg.writeStream.outputMode("update").foreachBatch(sink)
            .option("checkpointLocation", CK_AGG).trigger(availableNow=True).start())


progress = run(agg_query())
last = progress_summary(progress[-1])
print("após a captura:", {k: last[k] for k in ("batchId", "watermark", "stateRows", "droppedByWatermark")})
newest = static.agg(F.max("event_time")).first()[0]
print("evento mais novo visto:", static.agg(F.date_format(F.max("event_time"), "HH:mm:ss")).first()[0], "UTC")

# %%
def enwiki_minute(minute):
    return (spark.read.format("delta").load(AGG)
            .where((F.col("wiki") == "enwiki") & ~F.col("is_bot")
                   & (F.date_format("window_start", "HH:mm") == minute))
            .select("edits").first() or [0])[0]


fmt = "%Y-%m-%dT%H:%M:%S.000Z"
very_late = newest.timestamp() - 240
slightly_late = newest.timestamp() - 30
min_old, min_new = time.strftime("%H:%M", time.gmtime(very_late)), time.strftime("%H:%M", time.gmtime(slightly_late))
before = {m: enwiki_minute(m) for m in (min_old, min_new)}
late_rows = [{"meta": {"id": f"injetado-{i}", "dt": time.strftime(fmt, time.gmtime(t))}, "wiki": "enwiki",
              "bot": False, "type": "edit", "title": "Evento atrasado", "user": "demo"}
             for i, t in enumerate((very_late, slightly_late))]
late_file = LAND / "rc-99999999T999999999999-late.jsonl.gz"
with gzip.open(late_file, "wt") as fh:
    fh.writelines(json.dumps(r) + "\n" for r in late_rows)
os.utime(late_file, (2_000_000, 2_000_000))

p = progress_summary(run(agg_query())[-1])
after = {m: enwiki_minute(m) for m in (min_old, min_new)}
print(f"lote do arquivo atrasado: linhas lidas={p['numInputRows']}, "
      f"descartadas pelo watermark={p['droppedByWatermark']}")
print(f"enwiki/humanos {min_old} (4 min atrás): {before[min_old]} → {after[min_old]}  (descartado)")
print(f"enwiki/humanos {min_new} (30 s atrás):  {before[min_new]} → {after[min_new]}  (aceito)")

# %% [markdown]
# A tabela é a prova: a janela antiga não mudou, a recente ganhou +1 — e a métrica `numRowsDroppedByWatermark`
# marcou 1, o evento de 4 minutos atrás. Duas ressalvas sobre essa métrica: ela conta linhas **depois da agregação
# parcial** (vários eventos atrasados da mesma janela e chave contam como 1), e ela **soma a cada execução do plano
# do lote** — um `foreachBatch` que usa o DataFrame em duas ações sem `persist()` reexecuta a agregação e a métrica
# sai dobrada (aconteceu aqui: marcava 2 até o `merge_batch` passar a fazer `persist`). Use-a como alarme (> 0 =
# está descartando) e meça o efeito no dado.
#
# > 🎤 **Resposta de 30 s:** "Watermark é o contrato de atraso: 'espero até X por evento atrasado; depois disso,
# > fecho a janela e libero memória'. Eu escolho X medindo a distribuição real de atraso da fonte — p99, por exemplo
# > — e monitoro `numRowsDroppedByWatermark`. Dado que chega depois vai para um caminho de correção em batch, não
# > some em silêncio."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - O watermark é **global** da consulta: com várias fontes/partições, vale o **mínimo** entre elas por padrão
#   (`spark.sql.streaming.multipleWatermarkPolicy=min`); partição parada segura todo mundo.
# - Ele avança ao **fim** de cada micro-lote; a garantia é "nunca descarto antes do limite", não "descarto
#   exatamente no limite" (o Spark pode aceitar algo um pouco mais velho).
# - Recuperar o descartado: guardar a bronze completa (o evento atrasado está lá) e recalcular as janelas afetadas
#   num batch diário com MERGE — padrão *lambda-lite*.
# </details>
#
# **Trade-offs**
# - Watermark longo = resultado mais completo e mais estado (memória, checkpoint maior).
# - Watermark curto = menos estado e mais descarte; ruim para fontes com dispositivos offline (mobile, IoT).

# %% [markdown]
# ## 7. `foreachBatch` com MERGE idempotente
#
# **O que é.** `foreachBatch(fn)` entrega cada micro-lote como um DataFrame **batch** comum, mais o `batchId`. Dentro
# dele vale tudo de batch: MERGE, escrever em dois destinos, chamar API.
#
# **Por que importa.** O sink Delta nativo só faz append/complete. Para manter uma tabela de agregados corrigível
# (modo `update`) é preciso MERGE — e o `foreachBatch` é a porta. Mas ele é **at-least-once**: após uma falha, o
# mesmo `batchId` pode rodar de novo. A função tem de ser idempotente.
#
# **Como funciona.** `merge_batch` faz `MERGE ... ON (window_start, wiki, is_bot)`; em `update`, cada linha traz o
# valor **atual** da janela (não um incremento), então aplicar duas vezes dá o mesmo resultado. Provamos
# reaplicando um lote.

# %%
hist = DeltaTable.forPath(spark, AGG).history().select("version", "operation").orderBy("version")
print("operações no destino:", [(r.version, r.operation) for r in hist.collect()][:6], "…")
snapshot = sorted(map(tuple, spark.read.format("delta").load(AGG).collect()))
replay = spark.read.format("delta").load(AGG).limit(50)  # "o mesmo lote de novo"
sink(replay, 999)
again = sorted(map(tuple, spark.read.format("delta").load(AGG).collect()))
print(f"linhas antes: {len(snapshot)} | depois de reaplicar: {len(again)} | idênticas: {snapshot == again}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "`foreachBatch` me dá o micro-lote como DataFrame batch para fazer MERGE. Como ele é
# > at-least-once, a escrita tem de ser idempotente: MERGE com valor absoluto por chave, ou, para append, as opções
# > `txnAppId`/`txnVersion` do Delta com o `batchId` — o Delta ignora a versão repetida."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Append idempotente dentro de `foreachBatch`:
#   `df.write.format("delta").option("txnAppId", "minha_query").option("txnVersion", batch_id).mode("append")`.
# - **Incremento** (`SET t.n = t.n + s.n`) **não** é idempotente: reaplicar soma duas vezes. Evite, ou guarde o
#   último `batchId` aplicado por chave.
# - Escrever em dois destinos no mesmo `foreachBatch`: faça `df.persist()` antes, senão o lote é recomputado
#   (e a fonte relida) para cada escrita.
# - Desde o Spark 4 há `transformWithState` (estado arbitrário com TTL) para lógica que janela não cobre.
# </details>
#
# **Trade-offs**
# - MERGE por micro-lote é mais caro que append; com lotes muito frequentes, o custo de commit domina.
# - O destino em `update` mostra valores parciais (janela ainda aberta) — o consumidor precisa saber disso.

# %% [markdown]
# ## 8. Triggers: `availableNow` × `processingTime`, e o state store
#
# **O que é.**
# - `trigger(availableNow=True)`: processa **tudo o que está disponível** (em vários micro-lotes, respeitando
#   `maxFilesPerTrigger`) e **para**. É "batch incremental" com as garantias do streaming.
# - `trigger(processingTime="10 seconds")`: roda para sempre, um micro-lote a cada intervalo.
# - O **state store** guarda o estado das agregações (janelas abertas, ids de dedupe) entre micro-lotes. Padrão:
#   em memória no executor + snapshot no checkpoint (HDFS-backed). Alternativa: **RocksDB**, em disco local,
#   para estado grande.
#
# **Por que importa.** A escolha do trigger é, antes de tudo, escolha de **custo**: cluster ligado 24 h ×
# cluster ligado 5 min por hora. E o state store define quanto estado cabe antes de o executor estourar memória.
#
# **Como funciona.** Mesma consulta, com `processingTime` e o provider **RocksDB** (que roda local também):

# %%
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
agg = edits_per_window(parse_wiki(read_wiki_stream(spark, str(LAND), max_files_per_trigger=4)),
                       "1 minute", watermark="1 minute")
q = (agg.writeStream.outputMode("update").format("noop")
     .option("checkpointLocation", str(DEMO / "_ck" / "rocksdb")).trigger(processingTime="2 seconds").start())
q.processAllAvailable()  # bloqueia até consumir o que existe — o stream continua vivo
print("ainda ativo depois de processar tudo?", q.isActive, "| status:", q.status["message"])
lp = next(p for p in reversed(q.recentProgress) if p["numInputRows"])
q.stop()
op = lp["stateOperators"][0]
rocks = {k: v for k, v in op["customMetrics"].items() if k.startswith("rocksdb")}
print("provider:", "RocksDB" if rocks else "HDFS-backed", "| linhas de estado:", op["numRowsTotal"],
      "| memória:", op["memoryUsedBytes"], "bytes")
print("algumas métricas do RocksDB:", dict(list(sorted(rocks.items()))[:4]))
spark.conf.unset("spark.sql.streaming.stateStore.providerClass")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Se o negócio aceita latência de minutos, uso `availableNow` agendado: mesmas
# > garantias de streaming, cluster ligado só o tempo de processar. `processingTime` contínuo só quando a latência
# > paga o cluster 24 h. Para estado grande, RocksDB como state store: o estado sai do heap da JVM."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `trigger(once=True)` está **depreciado**: processava tudo num lote só (sem respeitar limites de taxa);
#   `availableNow` substitui.
# - Trocar o provider do state store num checkpoint existente não é permitido — decida antes da primeira execução.
# - RocksDB com *changelog checkpointing* grava só o delta do estado por lote (checkpoint mais rápido) — recomendado
#   no Databricks ☁️.
# - Trigger contínuo (`continuous`) do Spark OSS é experimental; o Databricks tem um modo de **real-time** próprio
#   para latência sub-segundo ☁️ — confira a disponibilidade na sua versão antes de prometer.
# </details>
#
# **Trade-offs / quando NÃO fazer streaming**
# - Latência exigida em horas → batch agendado (ou `availableNow`, que é o mesmo custo com checkpoint de graça).
# - Lógica que precisa ver o conjunto inteiro (ranking global, deduplicação de histórico completo) → batch.
# - Time sem experiência operacional em streaming: estado, checkpoint e watermark são novos modos de falhar.

# %% [markdown]
# ## 9. Observabilidade: `StreamingQueryProgress`
#
# **O que é.** A cada micro-lote o Spark publica um **progress** (JSON): linhas lidas, taxa de entrada × de
# processamento, duração por fase, watermark, linhas de estado, descartes, offsets de cada fonte.
#
# **Por que importa.** É o painel de saúde do stream. A regra de ouro: se `inputRowsPerSecond` fica
# consistentemente acima de `processedRowsPerSecond`, o stream está **atrasando** (*falling behind*) e o lag cresce.
#
# **Como funciona.** `query.recentProgress` (últimos lotes), `query.lastProgress` e, para produção, um
# `StreamingQueryListener` que envia cada progress para o sistema de métricas.

# %%
for p in progress[:4]:
    print(progress_summary(p))
print("\nfases do último lote (ms):", progress[-1]["durationMs"])
print("fonte:", {k: progress[-1]["sources"][0][k] for k in ("description", "numInputRows")})

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Monitoro input × processed rows por segundo, duração do trigger contra o intervalo,
# > watermark andando, linhas de estado e descartes. Em produção, um `StreamingQueryListener` manda isso para o
# > sistema de métricas e alerta quando o lag cresce."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `triggerExecution` maior que o intervalo do trigger = lotes encavalando: aumente o cluster ou o
#   `maxFilesPerTrigger`/`maxOffsetsPerTrigger`, ou reveja o estado.
# - `stateRows` crescendo sem parar = watermark ausente/errado ou chave de alta cardinalidade.
# - No Databricks, a aba de streaming da UI e as *system tables* (notebook 15) mostram o mesmo sem código.
# </details>
#
# **Trade-offs**
# - Listener síncrono e pesado atrasa o driver; envie de forma assíncrona e enxuta.

# %% [markdown]
# ## 10. Juntando: o pipeline da Silver
#
# **O que é.** O mesmo código, apontado para os caminhos oficiais: `landing/wikimedia` → `silver/wiki_changes`
# (eventos deduplicados, append exactly-once) e `silver/wiki_edits_per_minute` (agregado por MERGE), cada consulta
# com o seu checkpoint em `_checkpoints/`.
#
# **Por que importa.** É o formato de produção: idempotente, reexecutável e agendável (`availableNow`).
#
# **Como funciona.** Copiamos a fixture para a landing oficial (se ainda não estiver lá) e rodamos. Reexecutar o
# notebook não duplica nada: o checkpoint sabe o que já foi lido.

# %%
LANDING = Path(s.path("landing", "wikimedia"))
LANDING.mkdir(parents=True, exist_ok=True)
for f in FILES:
    if not (LANDING / f.name).exists():
        shutil.copy2(f, LANDING / f.name)

events = dedup_events(parse_wiki(read_wiki_stream(spark, str(LANDING), max_files_per_trigger=6)))
q1 = (events.writeStream.format("delta").outputMode("append").trigger(availableNow=True)
      .option("checkpointLocation", s.checkpoint("wiki_changes")).start(s.path("silver", "wiki_changes")))
p1 = run(q1)
# (agrega sem dedupe: dois operadores com estado em sequência têm restrições de output mode)
agg = edits_per_window(parse_wiki(read_wiki_stream(spark, str(LANDING), max_files_per_trigger=6)),
                       "1 minute", watermark="1 minute")
q2 = (agg.writeStream.outputMode("update").foreachBatch(merge_batch(s.path("silver", "wiki_edits_per_minute"), KEYS))
      .option("checkpointLocation", s.checkpoint("wiki_edits_per_minute")).trigger(availableNow=True).start())
p2 = run(q2)
n1 = spark.read.format("delta").load(s.path("silver", "wiki_changes")).count()
n2 = spark.read.format("delta").load(s.path("silver", "wiki_edits_per_minute")).count()
print(f"nesta execução: {sum(p['numInputRows'] for p in p1)} eventos novos lidos")
print(f"silver.wiki_changes: {n1} linhas | silver.wiki_edits_per_minute: {n2} linhas")

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# **Azure Event Hubs pelo endpoint Kafka** — o produtor publica em Event Hubs; o Spark lê como Kafka (sem conector
# próprio). Fonte replayable (offsets + retenção), então exactly-once continua valendo:
#
# ```python
# conn = dbutils.secrets.get("kv", "eventhubs-conn")          # connection string no Key Vault
# raw = (spark.readStream.format("kafka")
#        .option("kafka.bootstrap.servers", "ns-osslh.servicebus.windows.net:9093")
#        .option("subscribe", "wiki-recentchange")             # nome do event hub = tópico
#        .option("kafka.security.protocol", "SASL_SSL")
#        .option("kafka.sasl.mechanism", "PLAIN")
#        .option("kafka.sasl.jaas.config",
#                'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
#                f'username="$ConnectionString" password="{conn}";')
#        .option("startingOffsets", "earliest")
#        .option("maxOffsetsPerTrigger", 50_000)
#        .load())
# events = raw.select(F.from_json(F.col("value").cast("string"), WIKI_SCHEMA).alias("e")).select("e.*")
# ```
#
# (Em produção, prefira autenticação Entra ID — OAUTHBEARER com identidade gerenciada/service principal — em vez de
# connection string.)
#
# **Auto Loader como fonte de streaming** (substitui o file source; escala para milhões de arquivos, evolui schema):
#
# ```python
# (spark.readStream.format("cloudFiles")
#    .option("cloudFiles.format", "json")
#    .option("cloudFiles.schemaLocation", "abfss://lake@<conta>.dfs.core.windows.net/_schemas/wiki")
#    .option("cloudFiles.schemaHints", "meta STRUCT<id STRING, dt STRING>, bot BOOLEAN")
#    .load("abfss://lake@<conta>.dfs.core.windows.net/landing/wikimedia/")
#  .writeStream.trigger(availableNow=True)
#    .option("checkpointLocation", ".../_checkpoints/wiki_changes")
#    .toTable("oss_lakehouse_prod.silver.wiki_changes"))
# ```
#
# **State store e triggers no cluster:**
#
# ```python
# spark.conf.set("spark.sql.streaming.stateStore.providerClass",
#                "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
# spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
# ```
#
# - **Lakeflow Spark Declarative Pipelines** (antigo DLT): o mesmo pipeline declarado como *streaming tables*, com
#   checkpoint e retries gerenciados (notebook 08).
# - **Custo**: job agendado com `availableNow` em *job compute* (ou serverless) custa uma fração de um cluster
#   contínuo; streaming contínuo só se a latência for requisito de negócio.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Como Structured Streaming garante exactly-once?**
# <details><summary>Resposta</summary>
# Fonte replayable (reler um intervalo de offsets), checkpoint com write-ahead log de offsets e commit por micro-lote,
# e sink idempotente (Delta guarda <code>(queryId, batchId)</code> e ignora regravação). Falha no meio → o mesmo lote é
# refeito sem duplicar.
# </details>
#
# **2. O que é watermark e o que acontece com evento que chega depois dele?**
# <details><summary>Resposta</summary>
# Máximo event time visto menos o atraso tolerado. Evento mais velho é descartado (métrica
# <code>numRowsDroppedByWatermark</code>), e janelas que terminaram antes dele são finalizadas e saem do estado.
# </details>
#
# **3. Event time × processing time?**
# <details><summary>Resposta</summary>
# Event time é quando o fato ocorreu (vem no dado); processing time é quando o cluster processou. Agregação de negócio
# usa event time para o resultado não depender de quando o job rodou.
# </details>
#
# **4. Diferença entre os output modes append, update e complete?**
# <details><summary>Resposta</summary>
# Append: só linhas finais (com agregação, exige watermark). Update: só linhas alteradas no lote (casa com MERGE).
# Complete: a tabela de resultado inteira a cada lote (só para resultado pequeno).
# </details>
#
# **5. Por que meu stream em append com janela não escreve as últimas janelas?**
# <details><summary>Resposta</summary>
# Porque elas só saem quando o watermark passa do fim delas, e o watermark só avança com dado novo. Com
# <code>availableNow</code> o stream termina antes; elas ficam no estado até a próxima execução.
# </details>
#
# **6. <code>foreachBatch</code> é exactly-once?**
# <details><summary>Resposta</summary>
# Não por si: é at-least-once — o mesmo <code>batchId</code> pode rodar de novo após falha. Fica exactly-once se a escrita for
# idempotente (MERGE com valor absoluto, ou <code>txnAppId</code>/<code>txnVersion</code> no append Delta).
# </details>
#
# **7. Quando usar <code>availableNow</code> em vez de um stream contínuo?**
# <details><summary>Resposta</summary>
# Quando latência de minutos/horas basta: agenda o job, ele processa o incremental e desliga — mesmas garantias,
# custo de batch. Contínuo só quando a latência paga o cluster ligado.
# </details>
#
# **8. Meu job de streaming está ficando para trás. Como você diagnostica?**
# <details><summary>Resposta</summary>
# No progress: input rate acima de processed rate, <code>triggerExecution</code> maior que o intervalo, estado crescendo.
# Causas: skew, estado sem watermark, lote grande demais, sink lento (MERGE sem clustering). Ações: escalar, limitar
# taxa, corrigir watermark, RocksDB, clusterizar o destino.
# </details>
#
# **9. Para que serve o RocksDB como state store?**
# <details><summary>Resposta</summary>
# Guardar estado grande fora do heap da JVM (em disco local, com cache), evitando GC longo e OOM; com changelog
# checkpointing o checkpoint por lote fica incremental.
# </details>
#
# **10. Como ler Azure Event Hubs no Spark?**
# <details><summary>Resposta</summary>
# Pelo endpoint compatível com Kafka (porta 9093, SASL_SSL): <code>format("kafka")</code> com o namespace como bootstrap e o
# event hub como tópico; credencial do Key Vault ou, melhor, Entra ID. Fonte replayable dentro da retenção.
# </details>
#
# **11. Como deduplicar eventos num stream sem estado infinito?**
# <details><summary>Resposta</summary>
# <code>withWatermark</code> + <code>dropDuplicatesWithinWatermark(["event_id"])</code>: cada id fica no estado só enquanto o watermark
# não passou. <code>dropDuplicates</code> sem watermark guarda todos os ids para sempre.
# </details>

# %% [markdown]
# ## Resumo
#
# - Exactly-once = fonte replayable + checkpoint + sink idempotente; `foreachBatch` só é exactly-once se a escrita for
#   idempotente (MERGE com valor absoluto, `txnVersion`).
# - Agregue por **event time**; o watermark limita o estado e descarta o atrasado — prove com
#   `numRowsDroppedByWatermark`.
# - Output mode casa com o sink: append (janela fechada) → tabela imutável; update → MERGE; complete → resultado pequeno.
# - `availableNow` é batch incremental com garantias de streaming — muitas vezes a escolha certa de custo.
# - Monitore input × processed rate, duração do trigger, watermark e linhas de estado.

# %%
spark.stop()
