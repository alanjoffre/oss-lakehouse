# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 05 · Bronze → Silver: tipagem, deduplicação, MERGE e SCD tipo 2
#
# > Prova que a Silver é **idempotente** (reprocessar não duplica), **incremental** (só lê o que é novo)
# > e que a dimensão de repositórios guarda o **histórico real de renomeações** — inclusive quando o
# > dado chega fora de ordem.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Python avançado | funções puras e testadas em `oss_lakehouse.silver` e `oss_lakehouse.scd2` |
# | Databricks e processamento de dados | MERGE do Delta, `foreachBatch`, deletion vectors, Auto CDC ☁️ |
# | Arquitetura de pipelines | Medallion, idempotência, dado atrasado, reprocessamento |
# | Git / versionamento | lógica no pacote com testes (`tests/test_silver.py`, `tests/test_scd2.py`) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup

# %%
import shutil
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from oss_lakehouse.bronze import GH_EVENT_SCHEMA, add_ingestion_metadata
from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.scd2 import apply_scd2, scd2_as_of
from oss_lakehouse.silver import (
    PAYLOAD_SCHEMA,
    bronze_to_silver,
    build_silver_events,
    dedup_events,
    merge_events,
)
from oss_lakehouse.spark import get_spark

spark = get_spark("05")
s = get_settings()
BRONZE = s.path("bronze", "gh_events")
SILVER = s.path("silver", "gh_events")
DIM_REPO_SCD2 = s.path("silver", "dim_repo_scd2")

DEMO = Path(s.data_root) / "demo" / "05"  # tudo que é destrutivo acontece aqui
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True)

bronze = spark.read.format("delta").load(BRONZE)
print(f"bronze: {bronze.count():,} eventos")

# %% [markdown]
# ## 1. Bronze → Silver: tipar, achatar e extrair o payload
#
# **O que é** — a Silver é a camada "limpa e confiável" do *Medallion* (bronze = cópia fiel da fonte;
# silver = dado tipado, deduplicado e com regras de negócio leves; gold = modelo para consumo).
#
# **Por que importa** — na bronze tudo é texto: `id` é string, `created_at` é string ISO-8601 e o
# `payload` é um JSON bruto cujo formato muda por tipo de evento. Ninguém consegue somar, filtrar por data
# ou juntar tabelas com segurança em cima disso.
#
# **Como funciona** — `bronze_to_silver` (em `oss_lakehouse/silver.py`):
#
# ```text
# id (string) ─────────────► event_id (bigint)
# created_at (string) ─────► created_at (timestamp UTC) → event_date, event_hour
# actor{id,login,…} ───────► actor_id, actor_login, is_bot (login termina em "[bot]")
# repo{id,name,url} ───────► repo_id, repo_name, repo_owner
# payload (JSON string) ───► from_json(schema PARCIAL) → action, ref, pr_number, issue_title, …
# (tudo) ──────────────────► _content_hash (sha256 do evento) — decide se o MERGE reescreve
# ```

# %%
silver_preview = bronze_to_silver(bronze)
for name, dtype in silver_preview.dtypes[:9]:
    print(f"{name:<14} {dtype}")
print("…", len(silver_preview.columns), "colunas no total")

(
    silver_preview.filter("event_type IN ('PullRequestEvent', 'IssuesEvent', 'PushEvent')")
    .groupBy("event_type").agg(F.min_by(F.struct("event_id", "created_at", "repo_name", "action", "ref",
                                                  "pr_number", "issue_number"), "event_id").alias("ex"))
    .select("event_type", "ex.*").show(truncate=28)
)

# %% [markdown]
# ### `from_json` com schema parcial × `get_json_object`
#
# Duas formas de ler JSON guardado em string:
#
# - `get_json_object(payload, '$.a.b')` — cada chamada **faz o parse do JSON de novo**. Dez campos = dez parses.
# - `from_json(payload, schema)` — **um parse** por linha, devolvendo um `struct`; campos fora do schema são
#   ignorados. Declarar só o que se usa (schema *parcial*) mantém o custo baixo e o contrato explícito.
#
# Medimos as duas extraindo os mesmos campos da hora 12 (gravação `noop` = executa tudo e descarta).

# %%
h12 = bronze.filter(F.col("_source_file").endswith("-12.json.gz")).cache()
h12.count()
paths = ["action", "ref", "ref_type", "push_id", "head", "number", "pull_request.id", "pull_request.number",
         "pull_request.base.ref", "pull_request.head.ref", "issue.number", "issue.title", "issue.state",
         "review.state", "release.tag_name"]


def medir(df) -> float:
    t0 = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    return time.perf_counter() - t0


via_get = h12.select(*[F.get_json_object("payload", f"$.{p}").alias(p.replace(".", "_")) for p in paths])
via_from = h12.select(F.from_json("payload", PAYLOAD_SCHEMA).alias("p")).select(
    *[F.col(f"p.{p}").alias(p.replace(".", "_")) for p in paths])
medir(via_from)  # aquecimento (JIT da JVM, cache do arquivo)
t_get, t_from = medir(via_get), medir(via_from)
print(f"{len(paths)} campos · get_json_object: {t_get:.1f}s · from_json: {t_from:.1f}s")
iguais = via_get.select(F.col("pull_request_number").cast("long").alias("n")).exceptAll(
    via_from.select(F.col("pull_request_number").alias("n"))).count()
print("linhas divergentes em pull_request.number:", iguais)

# %% [markdown]
# Dado real tem surpresas: no GH Archive de 2026 o `PullRequestEvent` vem **enxuto** — sem `title` e sem
# `merged`. O merge aparece como `action = 'merged'`. Quem escreveu o pipeline lendo a documentação antiga
# procuraria `payload.pull_request.merged = true` e acharia **zero** PRs mergeados:

# %%
pr = bronze.filter("type = 'PullRequestEvent'")
print("PRs com payload.pull_request.merged preenchido:",
      pr.filter(F.get_json_object("payload", "$.pull_request.merged").isNotNull()).count())
pr.groupBy(F.get_json_object("payload", "$.action").alias("action")).count().orderBy(F.desc("count")).show()

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Na Silver eu tipo tudo (id vira bigint, data vira timestamp UTC), achato os
# > structs e extraio do JSON só os campos que o negócio usa, com `from_json` e schema parcial — um parse por
# > linha em vez de um por campo. O payload bruto fica na bronze: se amanhã eu precisar de outro campo,
# > reprocesso a partir dela. E valido o dado real, não a documentação: aqui o formato do evento de PR mudou."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que não inferir o schema do payload?** Inferência lê o dado inteiro (ou uma amostra) e gera um
#   struct gigante com a união de todos os tipos de evento; um campo novo muda o schema em silêncio.
# - **VARIANT** (Spark 4 / Databricks): tipo semiestruturado binário — guarda o JSON já "parseado", e
#   `variant_get(v, '$.a.b', 'int')` lê um caminho sem reparsear o texto. É a alternativa moderna à string
#   JSON na bronze/silver quando o formato varia muito (notebook 03 mostra rodando local).
# - **`_content_hash` com `to_json(struct(...))`**: `concat_ws` pula nulos, então `('a', null, 'b')` e
#   `('a', 'b', null)` gerariam o mesmo texto. `to_json` preserva a posição e o nulo.
# - **`is_bot` por sufixo `[bot]`** é heurística: pega GitHub Apps, não pega conta humana usada por script.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Schema parcial esconde campos novos: se o negócio precisar deles, é mudança de código (e de contrato).
# - `get_json_object` ainda é útil para exploração pontual (1 campo, ad hoc) — não para pipeline.

# %% [markdown]
# ## 2. Deduplicação: por que `row_number` e não `dropDuplicates`
#
# **O que é** — garantir 1 linha por chave de negócio (`event_id`).
#
# **Por que importa** — a bronze é *at-least-once* (pelo menos uma vez): arquivo reenviado pela fonte,
# backfill sobreposto, job reexecutado depois de falha. Nesta amostra não há duplicata, mas o pipeline
# não pode depender disso:

# %%
print(f"bronze: {bronze.count():,} linhas · {bronze.select('id').distinct().count():,} ids distintos")

# %% [markdown]
# **Como funciona** — simulamos uma **reentrega**: 1.000 eventos da hora 12 chegam de novo, 1 h depois,
# de outro arquivo. A regra de negócio é "fica a cópia mais recente".

# %%
silver_h12 = bronze_to_silver(h12)
reentrega = (
    silver_h12.orderBy("event_id").limit(1000)
    .withColumn("_ingested_at", F.col("_ingested_at") + F.expr("INTERVAL 1 HOUR"))
    .withColumn("_source_file", F.lit("reentrega/2026-10-01-12.json.gz"))
)
com_dup = silver_h12.unionByName(reentrega)

por_row_number = dedup_events(com_dup)
por_drop_dup = com_dup.dropDuplicates(["event_id"])
for nome, df in [("row_number", por_row_number), ("dropDuplicates", por_drop_dup)]:
    total = df.count()
    da_reentrega = df.filter(F.col("_source_file").startswith("reentrega")).count()
    print(f"{nome:<15} linhas={total:,}  cópias da reentrega mantidas={da_reentrega:,} de 1.000")

# %% [markdown]
# Os dois chegam à mesma **contagem** — mas só o `row_number` com ordem explícita garante **qual** cópia
# sobrevive: ele ficou com as 1.000 cópias novas, como a regra pede. O `dropDuplicates` mantém "a primeira
# que encontrar" — veja na saída quantas das cópias novas ele manteve nesta execução. O número depende da
# ordem física das partições: muda com o número de arquivos, com o AQE, com a versão do Spark. Em
# auditoria, "deu certo (ou errado) por sorte" não é resposta.
#
# Em **streaming** a conta é outra: o estado da deduplicação não pode crescer para sempre. Para isso existe
# `dropDuplicatesWithinWatermark` (Spark 3.5+), que esquece as chaves mais velhas que a *watermark*
# (marca d'água: "não espero dado com mais de X de atraso"). Rodando (🧪): a amostra de 2.000 eventos
# entregue **duas vezes**, em dois arquivos, um arquivo por micro-lote — a segunda entrega é toda duplicata:

# %%
dup_dir = DEMO / "landing_duplicada"
dup_dir.mkdir()
amostra_gz = PROJECT_ROOT / "tests" / "fixtures" / "gharchive" / "2026-10-01-12-sample.json.gz"
for nome in ("entrega_1.json.gz", "entrega_2.json.gz"):
    shutil.copy(amostra_gz, dup_dir / nome)
q = (
    spark.readStream.schema(GH_EVENT_SCHEMA).option("maxFilesPerTrigger", 1).json(str(dup_dir))
    .withColumn("ts", F.to_timestamp("created_at"))
    .withWatermark("ts", "2 hours")
    .dropDuplicatesWithinWatermark(["id"])  # estado limitado a ~2 h de chaves
    .writeStream.format("delta").option("checkpointLocation", str(DEMO / "_ck" / "dedup_stream"))
    .trigger(availableNow=True).start(str(DEMO / "dedup_stream"))
)
q.awaitTermination()
lotes = [(p.numInputRows, p.stateOperators[0].numRowsTotal) for p in q.recentProgress if p.numInputRows]
print("por micro-lote (linhas lidas, chaves no estado):", lotes)
print("gravadas:", f"{spark.read.format('delta').load(str(DEMO / 'dedup_stream')).count():,}")

# %% [markdown]
# O segundo micro-lote leu as mesmas linhas e não gravou nenhuma: as chaves ainda estavam no estado. Quando a
# watermark passar de `ts + 2 h`, a chave é esquecida — uma cópia que chegue **depois** disso entraria de
# novo (é o preço de limitar o estado; `dropDuplicates` em streaming guardaria todas as chaves para sempre).
#
# Aqui a Silver usa outra estratégia, que não precisa de estado: **dedup dentro do micro-lote**
# (`row_number`) **+ MERGE** contra a tabela (seção 3). O MERGE é a deduplicação "contra o passado".
#
# > 🎤 **Resposta de 30 s:** "`dropDuplicates` não é determinístico sobre qual linha fica. Quando a regra é
# > 'a mais recente', uso `row_number` sobre a chave ordenando por `_ingested_at` com desempate. Em
# > streaming, `dropDuplicatesWithinWatermark` para limitar o estado. E entre lotes, quem deduplica é o
# > MERGE na chave."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `row_number` exige um *shuffle* (redistribuir dados entre máquinas) pela chave — custo igual ao do
#   `dropDuplicates`. Para dedup global numa tabela grande, faça só sobre o lote novo + MERGE.
# - `qualify row_number() over (...) = 1` é a versão SQL (Databricks SQL e Spark 4 suportam `QUALIFY`).
# - Duplicata "exata" (todas as colunas iguais) × duplicata "de chave" (mesmo id, conteúdo diferente):
#   a segunda é a perigosa — exige regra de qual versão vence (mais recente, maior sequência, fonte prioritária).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Se a fonte garante unicidade e o MERGE já cobre, dedup no lote é redundante (mas barata em lote pequeno).
# - `dropDuplicates` sem ordem está ok quando as cópias são idênticas (ex.: mesma linha lida 2x).

# %% [markdown]
# ## 3. MERGE idempotente e incremental
#
# **O que é** — `MERGE INTO` (*upsert*: atualiza se existe, insere se não) do Delta, alimentado por um
# stream da bronze com `foreachBatch` (cada micro-lote vira um DataFrame comum, onde cabe um MERGE).
#
# **Por que importa** — *idempotência* (rodar N vezes = rodar 1 vez) é o que permite reexecutar um job que
# falhou no meio sem medo. E *incremental* é o que mantém o custo proporcional ao dado **novo**, não ao total.
#
# **Como funciona** — `build_silver_events`:
#
# ```text
# bronze (Delta) ──readStream──► micro-lote ──bronze_to_silver──► dedup_events ──MERGE──► silver
#                    │                                                              │
#                    └── checkpoint: "já li até a versão N da bronze"               └── ON event_id (+ data)
#                                                                                      WHEN MATCHED AND hash mudou → UPDATE
#                                                                                      WHEN NOT MATCHED → INSERT
# ```
#
# Duas camadas de proteção: o **checkpoint** evita reler o que já foi lido; o **MERGE** garante que, se
# algo for relido (checkpoint perdido, reprocessamento proposital), nada duplica.
#
# Demonstração numa bronze de DEMO que começa só com a hora 12:

# %%
demo_bronze, demo_silver = str(DEMO / "bronze"), str(DEMO / "silver_gh_events")
ck_principal, ck_novo = str(DEMO / "_ck" / "principal"), str(DEMO / "_ck" / "reprocesso")
h12.write.format("delta").partitionBy("event_date").save(demo_bronze)


def rodar(ckpt: str, rotulo: str) -> None:
    t0 = time.perf_counter()
    q = build_silver_events(spark, demo_bronze, demo_silver, ckpt)
    lidas = sum(p.numInputRows for p in q.recentProgress)
    tabela = DeltaTable.forPath(spark, demo_silver)
    versao = tabela.history(1).select("version").first()[0]
    n = tabela.toDF().count()
    print(f"{rotulo:<34} numInputRows={lidas:>7,}  silver={n:>7,}  versão={versao}  "
          f"({time.perf_counter() - t0:.0f}s)")


rodar(ck_principal, "1ª carga (hora 12)")
rodar(ck_principal, "de novo, mesmo checkpoint")
rodar(ck_novo, "reprocessar tudo (checkpoint novo)")

# %% [markdown]
# Leitura da saída:
# - **`numInputRows` vem em dobro** (184.646 = 2 × 92.323 linhas da hora 12). Não é dado duplicado — a silver
#   tem 92.323. É a métrica: dentro do `foreachBatch` o lote é usado por duas ações (coletar as datas para a
#   poda e o MERGE) e o progresso do stream soma a leitura da fonte em cada uma, mesmo com o lote em
#   `persist`. Armadilha clássica de monitoramento: volume se confere na tabela (ou no `operationMetrics` do
#   MERGE, abaixo), não em `numInputRows` de `foreachBatch`;
# - **mesmo checkpoint** → o stream não lê nada (o checkpoint sabe que já processou aquela versão da bronze);
# - **checkpoint novo** → relê a bronze inteira, mas o MERGE encontra todos os `event_id` com o mesmo hash:
#   nenhuma linha muda e **o Delta nem grava versão nova** (repare que a versão não andou).
#
# Agora chega a hora 13 na bronze. Só ela é lida:

# %%
bronze.filter(F.col("_source_file").endswith("-13.json.gz")).write.format("delta").mode("append").save(demo_bronze)
rodar(ck_principal, "chegou a hora 13")
DeltaTable.forPath(spark, demo_silver).history().select(
    "version", "operation", F.col("operationMetrics.numTargetRowsInserted").alias("inseridas"),
    F.col("operationMetrics.numTargetRowsUpdated").alias("atualizadas")).show()

# %% [markdown]
# E uma **correção na fonte**: 3 eventos reentregues com conteúdo diferente (hash novo). O MERGE atualiza
# exatamente esses 3 — não insere duplicata, não toca no resto:

# %%
corrigidos = (
    spark.read.format("delta").load(demo_silver).orderBy("event_id").limit(3)
    .withColumn("action", F.lit("corrigido_na_fonte")).withColumn("_content_hash", F.sha2(F.lit("v2"), 256))
)
m = merge_events(spark, corrigidos, demo_silver)
print({k: m[k] for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsCopied",
                         "numTargetFilesRemoved", "numTargetDeletionVectorsAdded")})
print("linhas com action corrigida:",
      spark.read.format("delta").load(demo_silver).filter("action = 'corrigido_na_fonte'").count())

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Leio a bronze como stream Delta com checkpoint e `trigger(availableNow)` —
# > processa só o que é novo e para, ótimo para job agendado. Em cada micro-lote, `foreachBatch` tipa,
# > deduplica e faz MERGE pela chave. `foreachBatch` é *at-least-once*: se o job cair depois do MERGE e antes
# > do checkpoint, o lote roda de novo — e o MERGE torna isso inofensivo. Ainda condiciono o UPDATE a
# > `hash mudou`, então reprocessar não reescreve nada."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que não `append` puro?** Append + checkpoint é exactly-once só enquanto o checkpoint existir.
#   Perdeu o checkpoint (ou precisou reprocessar um dia) → duplicou. MERGE tolera.
# - **Alternativa para append idempotente em `foreachBatch`**: as opções `txnAppId` + `txnVersion` do writer
#   Delta — o Delta ignora uma escrita com (appId, versão) já commitada. Serve quando não há chave de negócio.
# - **Insert-only MERGE** (`WHEN NOT MATCHED THEN INSERT` só) é o padrão para eventos imutáveis; o
#   `WHEN MATCHED AND hash mudou` aqui cobre correção na fonte.
# - **Condição do UPDATE com hash** evita o "MERGE que reescreve tudo": sem ela, todo reprocessamento
#   atualizaria todas as linhas casadas — reescrita total e versão nova sem mudança real.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - MERGE custa mais que append (join com o alvo). Em fato de altíssimo volume e fonte confiável, append com
#   `txnVersion` pode bastar.
# - Para CDC de verdade (insert/update/delete vindos de um banco), prefira Auto CDC (☁️, abaixo) — ele trata
#   ordenação por sequência e deletes.

# %% [markdown]
# ## 4. MERGE por dentro: por que fica lento e como resolver
#
# **O que é** — o MERGE do Delta acontece em duas fases:
#
# ```text
# Fase 1 (busca):   join fonte × alvo  → quais ARQUIVOS do alvo têm linhas casadas?
# Fase 2 (escrita): copy-on-write   → reescreve esses arquivos INTEIROS (linhas casadas mudam, o resto é copiado)
#                   merge-on-read   → com deletion vectors: marca as linhas antigas como apagadas num bitmap
#                                     e grava só as linhas novas; a leitura aplica o bitmap
# ```
#
# **Por que importa** — trocar 3 linhas pode custar reescrever gigabytes: o Parquet é imutável, então o
# *copy-on-write* copia o arquivo inteiro. É a causa nº 1 de "MERGE lento". A segunda é a fase 1 ler a
# tabela toda porque nada na condição permite podar arquivos.
#
# **Como funciona — evidência 1: deletion vectors.** O mesmo MERGE (atualizar 3 linhas) em duas cópias
# idênticas da silver de DEMO — 2 arquivos cada —, uma **com** e outra **sem** deletion vectors:

# %%
CHAVES_DV = ("numTargetRowsUpdated", "numTargetRowsCopied", "numTargetFilesRemoved", "numTargetFilesAdded",
             "numTargetBytesAdded", "numTargetDeletionVectorsAdded")
base_dv = spark.read.format("delta").load(demo_silver).repartition(2).cache()
tres = [r[0] for r in base_dv.orderBy(F.desc("event_id")).limit(3).select("event_id").collect()]
metricas_dv = {}
for dv in ("true", "false"):
    caminho = str(DEMO / f"silver_dv_{dv}")
    base_dv.write.format("delta").option("delta.enableDeletionVectors", dv).save(caminho)
    alvo = DeltaTable.forPath(spark, caminho)
    fonte = alvo.toDF().filter(F.col("event_id").isin(tres)).withColumn("_content_hash", F.sha2(F.lit("v3"), 256))
    alvo.alias("t").merge(fonte.alias("s"), "t.event_id = s.event_id").whenMatchedUpdateAll().execute()
    metricas_dv[dv] = alvo.history(1).first()["operationMetrics"]
    print(f"deletion vectors = {dv:<5}", {k.replace("numTarget", ""): metricas_dv[dv][k] for k in CHAVES_DV})
base_dv.unpersist()
mt = metricas_dv["false"]
print("tempos (ms) sem DV:", {k: mt[k] for k in ("scanTimeMs", "rewriteTimeMs", "executionTimeMs")})
print("chaves de operationMetrics com 'Skipping' (poda):", [k for k in mt if "Skipping" in k] or "nenhuma")

# %% [markdown]
# Sem deletion vectors, atualizar 3 linhas **reescreveu os arquivos onde elas estavam**: `RowsCopied` são as
# linhas que não mudaram e foram copiadas mesmo assim (a tabela inteira, menos 3), `BytesAdded` é o tamanho
# do que foi regravado (dezenas de MB). Com deletion vectors, `RowsCopied = 0` e nenhum arquivo removido: as
# 3 linhas antigas foram marcadas no bitmap (`DeletionVectorsAdded`) e só as 3 novas foram gravadas, em
# arquivos de poucos KB. O custo vai para a leitura (aplicar o bitmap) até o próximo `OPTIMIZE` ou *purge*
# consolidar os arquivos.
#
# (Funciona no MERGE do Delta open source 4.4 — conferido aqui; o agente de leitura precisa suportar a
# *table feature* `deletionVectors`.)
#
# **Evidência 2: poda de arquivos (*file pruning*).** O Delta guarda min/max de cada coluna por arquivo no
# log. Se a condição do MERGE tem um predicado **só sobre o alvo** com valores literais, a fase 1 lê só os
# arquivos cujo intervalo contém esses valores. `merge_events` faz isso com a data
# (`t.event_date IN (DATE'…')`). Para ver o efeito, uma cópia da silver em 8 arquivos ordenados por `event_id`:

# %%
ordenada = str(DEMO / "silver_8_arquivos")
(spark.read.format("delta").load(demo_silver).repartitionByRange(8, "event_id")
 .sortWithinPartitions("event_id").write.format("delta").save(ordenada))
t8 = spark.read.format("delta").load(ordenada)
ids = [r[0] for r in t8.orderBy("event_id").limit(3).select("event_id").collect()]
print("arquivos na tabela:", len(t8.inputFiles()))
print("arquivos lidos com  t.event_id IN (3 ids):", len(t8.filter(F.col("event_id").isin(ids)).inputFiles()))

# %% [markdown]
# É isso que a fase 1 ganha quando a condição carrega o predicado literal: ler 1 arquivo em vez de 8.
# Sem o literal (`ON t.event_id = s.event_id` puro), o Delta OSS não sabe de antemão quais ids virão e
# varre tudo. (No Databricks, o *Dynamic File Pruning* faz parte disso automaticamente ☁️.)
#
# > 🎤 **Resposta de 30 s:** "MERGE lento quase sempre é uma de duas coisas: varredura do alvo inteiro na
# > busca, ou reescrita de arquivos grandes por poucas linhas. Para a primeira, ponho na condição um
# > predicado do alvo — partição ou intervalo de data/id — e mantenho o dado agrupado pela chave (Liquid
# > Clustering ou Z-order) para os min/max serem seletivos. Para a segunda, ligo deletion vectors. E reduzo
# > a fonte: MERGE só do lote novo, deduplicado — fonte com chave duplicada nem roda."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Fonte com duas linhas para a mesma chave** → erro `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
#   Por isso o `dedup_events` vem antes do MERGE.
# - **Fase 1 é um join**: se a fonte é pequena, o Spark faz *broadcast* dela (manda a tabela inteira para
#   cada executor) e evita o *shuffle* do alvo. Fonte grande + alvo grande = sort-merge join caro.
# - **Concorrência**: dois MERGE no mesmo arquivo → `ConcurrentAppendException`/`ConcurrentDeleteReadException`.
#   Particionar/clusterizar para que jobs concorrentes toquem conjuntos de arquivos disjuntos, ou
#   serializar os writers. *Row-level concurrency* (Databricks, com deletion vectors + liquid) ☁️ reduz conflitos.
# - **Low Shuffle Merge** (Databricks ☁️): preserva a organização (Z-order) dos arquivos não modificados.
# - **Métricas**: tudo que mostrei saiu de `DESCRIBE HISTORY` → `operationMetrics` — é o primeiro lugar
#   para olhar num MERGE lento (`scanTimeMs`, `rewriteTimeMs`, `numTargetRowsCopied`). Cuidado ao copiar
#   receita da internet: `numTargetFilesBeforeSkipping`/`AfterSkipping` **não existem** no `operationMetrics`
#   do Delta OSS 4.4 (a célula acima lista "nenhuma") — aqui a poda foi medida por `inputFiles()` com o mesmo
#   predicado; no Databricks ela aparece no *query profile* ☁️.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Deletion vectors deixam a escrita barata e a leitura um pouco mais cara; precisam de `OPTIMIZE`/`REORG
#   … APPLY (PURGE)` periódico. Leitores antigos (fora do Delta 2.3+/3.x) não entendem a feature.
# - Predicado literal na condição só ajuda se o dado estiver **agrupado** pela coluna — com ids espalhados
#   aleatoriamente, todo arquivo contém o intervalo e nada é podado.

# %% [markdown]
# ## 5. Materializando a Silver oficial
#
# A mesma função, agora nos caminhos oficiais (`silver/gh_events`, checkpoint `_checkpoints/silver_gh_events`).
# É idempotente: na primeira vez carrega a bronze inteira; nas seguintes, só o que entrou depois.

# %%
t0 = time.perf_counter()
q = build_silver_events(spark)
silver = spark.read.format("delta").load(SILVER)
print(f"numInputRows nesta execução (em dobro, ver seção 3): {sum(p.numInputRows for p in q.recentProgress):,}  "
      f"({time.perf_counter() - t0:.0f}s)")
print(f"silver.gh_events: {silver.count():,} linhas · {silver.select('event_id').distinct().count():,} event_id distintos")

# %% [markdown]
# ## 6. SCD tipo 1, 2 e 3 — o conceito
#
# **O que é** — *Slowly Changing Dimension* (dimensão de mudança lenta): como uma tabela de dimensão
# reage quando um atributo muda. No GitHub, `repo.id` é estável e `repo.name` muda quando o dono renomeia
# ou **transfere** o repositório para outra conta/organização.
#
# | Tipo | O que faz | Pergunta que responde | Custo |
# |---|---|---|---|
# | 0 | nunca muda (valor original) | "qual era o nome no cadastro?" | nenhum |
# | 1 | sobrescreve | "qual o nome **hoje**?" | barato; perde o passado |
# | 2 | nova linha por versão (`valid_from`/`valid_to`/`is_current`) | "qual era o nome **quando** o evento aconteceu?" | cresce; joins com intervalo |
# | 3 | coluna `nome_anterior` | "qual o nome atual e o imediatamente anterior?" | só 1 nível de histórico |
#
# **Por que importa** — relatório "PRs mergeados por organização" feito com SCD1 atribui ao **dono atual**
# todo o trabalho feito antes da transferência. Às vezes é o que o negócio quer; às vezes é um erro de
# auditoria. A escolha é de negócio — o engenheiro precisa saber oferecer as duas.
#
# Um caso real do dia: o repositório `1385188237` foi **transferido** de conta.

# %%
REPO_EX = 1385188237
ex = silver.filter(F.col("repo_id") == REPO_EX)
ex.groupBy("repo_name").agg(F.min("created_at").alias("primeiro_evento"), F.max("created_at").alias("ultimo"),
                            F.count("*").alias("eventos")).orderBy("primeiro_evento").show(truncate=False)

# %%
w = Window.partitionBy("repo_id").orderBy("created_at")
obs = ex.select("repo_id", "repo_name", "created_at")
scd1 = obs.groupBy("repo_id").agg(F.max_by("repo_name", "created_at").alias("repo_name"))
mudancas = (obs.withColumn("anterior", F.lag("repo_name").over(w))
            .filter(F.col("anterior").isNull() | (F.col("anterior") != F.col("repo_name"))))
scd2_ex = (mudancas.select("repo_id", "repo_name", F.col("created_at").alias("valid_from"))
           .withColumn("valid_to", F.lead("valid_from").over(Window.partitionBy("repo_id").orderBy("valid_from")))
           .withColumn("is_current", F.col("valid_to").isNull()))
scd3 = mudancas.groupBy("repo_id").agg(F.max_by("repo_name", "created_at").alias("repo_name"),
                                       F.max_by("anterior", "created_at").alias("repo_name_anterior"))
print("SCD1:"); scd1.show(truncate=False)
print("SCD2:"); scd2_ex.show(truncate=False)
print("SCD3:"); scd3.show(truncate=False)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "SCD1 sobrescreve — responde 'como é hoje'. SCD2 versiona com `valid_from`,
# > `valid_to` e `is_current` — responde 'como era quando o fato aconteceu', ao custo de crescer e de exigir
# > join por intervalo. SCD3 guarda só o valor anterior numa coluna. Na prática, SCD2 para o que é auditável
# > (dono, preço, segmento do cliente) e SCD1 para correção de cadastro."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **SCD tipo 4**: histórico numa tabela separada (dimensão atual pequena + tabela de histórico).
#   **Tipo 6** = 1+2+3: versões (2), mais a coluna "valor atual" repetida em todas as versões (1), e o anterior (3).
# - **Quais atributos versionar?** Só os que o negócio quer ver no tempo. Versionar tudo (ex.: contador de
#   estrelas) explode o número de versões — isso é fato, não dimensão.
# - **Mini-dimensão**: atributo que muda muito (faixa de estrelas) sai da dimensão e vira dimensão própria.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - SCD2 em atributo volátil = tabela que cresce como fato. Use snapshot periódico.
# - Se ninguém consulta o passado, SCD2 é custo sem retorno — SCD1 + time travel do Delta pode bastar para auditoria curta.

# %% [markdown]
# ## 7. SCD tipo 2 genérico com MERGE: dado real, dado atrasado e consulta *as-of*
#
# **O que é** — `apply_scd2(spark, observacoes, caminho, keys, tracked, effective_col)` em
# `oss_lakehouse/scd2.py`: serve para qualquer dimensão (chave natural + atributos rastreados + data efetiva).
#
# **Por que importa** — o MERGE de SCD2 "de livro" assume que a mudança nova é sempre **mais recente** que a
# versão vigente: fecha a atual e abre outra. Mas dado atrasado existe (arquivo que chegou depois, backfill,
# fonte com relógio torto). Se chega uma observação **anterior** à versão vigente, o MERGE ingênuo abre uma
# versão "nova" com data velha — o histórico fica inconsistente (intervalos sobrepostos, `is_current` errado).
#
# **Como funciona** — para as chaves do lote, a linha do tempo é **recalculada** e o MERGE aplica a diferença:
#
# ```text
# histórico atual das chaves do lote ∪ observações novas
#    │ empate na mesma data efetiva → vence a observação nova
#    │ ordena por data efetiva; descarta observação igual à anterior (hash dos atributos)
#    │ valid_to = próxima data efetiva (lead) · is_current = valid_to nulo
#    │ sk = xxhash64(chave natural, valid_from)   ← determinística: reexecutar gera a mesma sk
#    ▼
# MERGE ON chave + valid_from
#    WHEN MATCHED AND (é versão que sumiu)        → DELETE
#    WHEN MATCHED AND (hash/valid_to/is_current mudou) → UPDATE
#    WHEN NOT MATCHED                              → INSERT
# ```
#
# Custo: o recálculo toca só as chaves presentes no lote (`left_semi` join), não a dimensão inteira.
#
# ### 7.1 A dimensão oficial a partir da Silver

# %%
obs_repo = silver.select("repo_id", "repo_name", "repo_owner", "created_at")
m = apply_scd2(spark, obs_repo, DIM_REPO_SCD2, keys=["repo_id"], tracked=["repo_name", "repo_owner"],
               effective_col="created_at")
print("esta execução:", {k: m.get(k, "0") for k in ("operation", "numTargetRowsInserted", "numTargetRowsUpdated",
                                                     "numTargetRowsDeleted")})
dim = spark.read.format("delta").load(DIM_REPO_SCD2)
versoes = dim.groupBy("repo_id").count()
print(f"dim_repo_scd2: {dim.count():,} versões · {versoes.count():,} repositórios · "
      f"{versoes.filter('count > 1').count()} repositórios com mais de uma versão")

# %%
w = Window.partitionBy("repo_id").orderBy("valid_from")
trocas = (dim.withColumn("nome_ant", F.lag("repo_name").over(w)).withColumn("dono_ant", F.lag("repo_owner").over(w))
          .filter("nome_ant IS NOT NULL"))
trocas.select(
    F.sum((F.col("dono_ant") != F.col("repo_owner")).cast("int")).alias("transferencias_de_dono"),
    F.sum((F.col("dono_ant") == F.col("repo_owner")).cast("int")).alias("renomeacoes_mesmo_dono"),
).show()
trocas.select("repo_id", F.col("nome_ant").alias("de"), F.col("repo_name").alias("para"),
              F.col("valid_from").alias("a_partir_de")).orderBy("a_partir_de").show(6, truncate=False)

# %% [markdown]
# ### 7.2 Carga incremental hora a hora — e a hora 11 chegando atrasada
#
# Numa dimensão de DEMO, aplicamos as observações das horas 12, 13 e 14 (uma de cada vez, como um job
# horário). Depois chega, **atrasado**, o arquivo da hora 11 (lido do `raw_cache`): observações *anteriores*
# a tudo que a dimensão já tem.

# %%
demo_dim = str(DEMO / "dim_repo_scd2")
cols = ["repo_id", "repo_name", "repo_owner", "created_at"]


def aplicar(df, rotulo: str) -> None:
    m = apply_scd2(spark, df.select(*cols), demo_dim, ["repo_id"], ["repo_name", "repo_owner"], "created_at")
    print(f"{rotulo:<26} inseridas={m.get('numTargetRowsInserted', '0'):>6}  "
          f"atualizadas={m.get('numTargetRowsUpdated', '0'):>3}  apagadas={m.get('numTargetRowsDeleted', '0'):>3}")


for h in (12, 13, 14):
    aplicar(silver.filter(F.col("event_hour") == h), f"hora {h}")

raw11 = spark.read.schema(GH_EVENT_SCHEMA).json(f"{s.data_root}/raw_cache/gharchive/2026-10-01-11.json.gz")
hora11 = bronze_to_silver(add_ingestion_metadata(raw11.select("*", "_metadata")).drop("_metadata")).cache()
print("observações da hora 11 sem repo_id (dado real):", hora11.filter("repo_id IS NULL").count(), "de",
      f"{hora11.count():,}")
REPO_CADEIA, REPO_TRANSF = 1385415744, 1147977624


def linha_do_tempo(*repo_ids: int) -> None:
    (spark.read.format("delta").load(demo_dim).filter(F.col("repo_id").isin(*repo_ids))
     .orderBy("repo_id", "valid_from").select("repo_id", "repo_name", "valid_from", "valid_to", "is_current")
     .show(truncate=False))


print("ANTES da hora 11:")
linha_do_tempo(REPO_CADEIA, REPO_TRANSF, REPO_EX)

# %%
aplicar(hora11, "hora 11 (atrasada)")
print("DEPOIS da hora 11:")
linha_do_tempo(REPO_CADEIA, REPO_TRANSF, REPO_EX)
aplicar(hora11, "hora 11 de novo")

# %% [markdown]
# O que aconteceu, caso a caso (todos reais):
# - `1385415744` — tinha 2 versões; a hora 11 revelou um **terceiro nome, anterior** aos dois. Virou a
#   primeira versão, fechada no instante em que o nome seguinte aparece.
# - `1147977624` — transferido de conta **antes** das 12h: o nome antigo só existe na hora 11. Ganhou uma
#   versão anterior e a versão que já existia continua vigente.
# - `1385188237` — na hora 11 já tinha o mesmo nome da primeira versão: nada muda de atributo, mas
#   `valid_from` **recua** para a hora 11 — a versão antiga é apagada e outra entra com a data correta.
#   É o caso da grande maioria das linhas "apagadas" na saída: todo repositório já conhecido que também
#   teve evento na hora 11 tem o início da primeira versão corrigido (e, com ele, a surrogate key — ver
#   trade-offs).
# - Rodar a hora 11 de novo: **zero** mudanças — idempotente.
# - **Achado do dado real:** a hora 11 traz um evento sem `repo.id` (um `ForkEvent` com `repo: {}`). Chave
#   nula é veneno para MERGE: `NULL = NULL` não é verdadeiro, a linha nunca "casa" e seria **reinserida a
#   cada execução**. `apply_scd2` descarta observação com chave ou data nula (teste em `tests/test_scd2.py`);
#   medir e pôr em quarentena é assunto do notebook 08.
#
# Um caso que o dado real deste dia não traz — a mudança atrasada **no meio** do histórico (A → C, depois
# chega B entre os dois) — fica num exemplo **sintético explícito** (também coberto em `tests/test_scd2.py`):

# %%
sint = str(DEMO / "scd2_sintetico")
esq = "repo_id long, repo_name string, observed_at string"  # texto → timestamp no fuso da sessão (UTC)
apply_scd2(spark, spark.createDataFrame([(1, "ana/a", "2026-10-01 10:00:00"),
                                         (1, "ana/c", "2026-10-01 14:00:00")], esq),
           sint, ["repo_id"], ["repo_name"], "observed_at")
m_sint = apply_scd2(spark, spark.createDataFrame([(1, "ana/b", "2026-10-01 12:00:00")], esq),
                    sint, ["repo_id"], ["repo_name"], "observed_at")
print("chegou B (12h) depois de A (10h) e C (14h):",
      {k: m_sint[k] for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")})
spark.read.format("delta").load(sint).orderBy("valid_from").select(
    "repo_name", "valid_from", "valid_to", "is_current").show()

# %% [markdown]
# ### 7.3 Consulta *point-in-time* (as-of)
#
# "Qual era o nome do repositório no instante T?" — a versão com `valid_from <= T < valid_to`
# (`valid_to` é **exclusivo**: no instante exato da troca já vale o nome novo).

# %%
for t in ("2026-10-01 11:30:00", "2026-10-01 13:00:00"):
    nomes = scd2_as_of(spark.read.format("delta").load(demo_dim), t).filter(
        F.col("repo_id").isin(REPO_CADEIA, REPO_TRANSF)).orderBy("repo_id").select("repo_id", "repo_name")
    print(t, [tuple(r) for r in nomes.collect()])

# %% [markdown]
# Em SQL (é o mesmo join que o fato faz na Gold, notebook 07):
#
# ```sql
# SELECT e.event_id, d.repo_name
# FROM silver.gh_events e
# JOIN silver.dim_repo_scd2 d
#   ON e.repo_id = d.repo_id
#  AND e.created_at >= d.valid_from
#  AND (e.created_at < d.valid_to OR d.valid_to IS NULL)
# ```
#
# > 🎤 **Resposta de 30 s:** "Minha SCD2 é um MERGE, mas não o ingênuo. Para as chaves do lote eu junto o
# > histórico atual com as observações novas, reordeno por data efetiva, colapso o que não mudou e recalculo
# > `valid_to` com `lead`. O MERGE aplica a diferença — insere, atualiza e apaga versões. Com isso, dado
# > atrasado se encaixa no lugar certo, e como a surrogate key é hash de chave + `valid_from`, reexecutar dá
# > zero mudanças. Aqui, a hora 11 chegando depois revelou renomeações reais anteriores."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que surrogate key por hash e não `IDENTITY`?** Coluna identidade (Databricks
#   `GENERATED ALWAYS AS IDENTITY`) gera número novo a cada carga — reconstruir a dimensão muda as chaves e
#   quebra os fatos já gravados. Hash de (chave natural, `valid_from`) é determinístico. Custo: risco de
#   colisão de 64 bits (desprezível em milhões de linhas; em bilhões, use sha2/128 bits).
# - **Primeira versão começa quando?** Aqui, na primeira observação. Kimball costuma usar uma data mínima
#   (1900-01-01) para que fatos anteriores ao primeiro registro ainda encontrem versão — escolha explícita.
# - **Empate de data efetiva com valores diferentes** (dois eventos no mesmo segundo, nomes diferentes):
#   a regra aqui é determinística (maior hash) mas arbitrária. Fonte com número de sequência resolve de verdade
#   — é o `sequence_by` do Auto CDC.
# - **Deletes na fonte**: esta função não fecha versão por ausência (repo apagado não gera evento). Para
#   isso: coluna `is_deleted` rastreada, ou `apply_as_deletes` no Auto CDC ☁️.
# - **`whenNotMatchedBySource`** (Delta 2.3+) permitiria apagar o que sumiu da fonte num *full refresh* —
#   aqui o "sumiu" é calculado por chave afetada, porque a fonte é incremental.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Recalcular a linha do tempo por chave é mais caro que o MERGE ingênuo; se a fonte garante ordem (CDC com
#   sequência monotônica), o ingênuo + filtro "só mais recente que a vigente" basta.
# - Chave com milhões de versões (atributo volátil) torna o recálculo pesado — sinal de que o atributo é fato.
# - `sk = hash(chave, valid_from)` **muda** quando dado atrasado recua o início de uma versão (visto acima).
#   Fato que já gravou a sk antiga precisa ser reprocessado na janela afetada — ou a primeira versão nasce
#   com data mínima fixa (1900-01-01), que nunca recua. Aqui a Gold é reconstruída a cada carga (notebook 07).

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# **Auto CDC (antigo `APPLY CHANGES INTO`)** no **Lakeflow Declarative Pipelines** (antigo Delta Live Tables)
# faz SCD1/SCD2 declarativamente, ordenando por `sequence_by` (resolve dado fora de ordem) e tratando deletes:
#
# ```python
# from pyspark import pipelines as dp   # no Databricks também existe o alias "import dlt"
#
# dp.create_streaming_table("silver_dim_repo")
# dp.create_auto_cdc_flow(
#     target="silver_dim_repo",
#     source="silver_repo_observations",      # stream com repo_id, repo_name, repo_owner, created_at
#     keys=["repo_id"],
#     sequence_by="created_at",
#     stored_as_scd_type=2,                   # gera __START_AT / __END_AT
#     track_history_column_list=["repo_name", "repo_owner"],
# )
# ```
#
# O Spark Declarative Pipelines **open source** (PySpark 4.1+) já tem `create_auto_cdc_flow`, mas a docstring
# local diz que só SCD tipo 1 é suportado — conferido abaixo (🧪):

# %%
import inspect

from pyspark import pipelines as dp

print([linha.strip() for linha in inspect.getdoc(dp.create_auto_cdc_flow).splitlines()
       if "stored_as_scd_type" in linha][0])

# %% [markdown]
# Outras diferenças na plataforma:
# - **Deletion vectors** habilitados por padrão em tabelas novas (configuração do workspace);
#   **Predictive Optimization** roda `OPTIMIZE`/`VACUUM` sozinho em tabelas gerenciadas do Unity Catalog.
# - **Dynamic File Pruning** e **Low Shuffle Merge** aceleram o MERGE sem mudar o código; **Photon** acelera a escrita.
# - **Row-level concurrency** (tabelas com deletion vectors): dois MERGE que tocam linhas diferentes do
#   mesmo arquivo não conflitam.
# - **Identity columns** (`GENERATED ALWAYS AS IDENTITY`) para surrogate key sequencial — com o cuidado de
#   não reconstruir a dimensão.
# - **Schema evolution no MERGE**: `MERGE WITH SCHEMA EVOLUTION INTO …` (ou `withSchemaEvolution()` na API).
# - Agendamento: um **Lakeflow Job** com a task da Silver dependendo da task da bronze (`availableNow` encaixa
#   em job agendado; o mesmo código roda contínuo trocando o trigger).

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. O que torna um pipeline idempotente? Como você prova?**
# <details><summary>Resposta</summary>
# Reexecutar com a mesma entrada produz o mesmo estado final. Aqui: checkpoint (não relê) + MERGE por chave
# (se reler, não duplica) + UPDATE condicionado a hash (não reescreve sem mudança). Prova: reprocessar com
# checkpoint novo e mostrar mesma contagem, mesmos ids distintos e nenhuma versão nova no log (seção 3).
# </details>
#
# **2. `dropDuplicates` ou `row_number`?**
# <details><summary>Resposta</summary>
# Mesma contagem, garantias diferentes: `dropDuplicates` mantém uma linha arbitrária; `row_number` com
# ordem explícita escolhe a correta (a mais recente, a de maior sequência). Em streaming,
# `dropDuplicatesWithinWatermark` limita o estado.
# </details>
#
# **3. Seu MERGE está lento. Por onde começa?**
# <details><summary>Resposta</summary>
# `DESCRIBE HISTORY` → `operationMetrics`: `scanTimeMs` alto = busca varrendo tudo (falta predicado podável na
# condição ou dado não agrupado pela chave); `numTargetRowsCopied` alto = copy-on-write de arquivos grandes
# (ligar deletion vectors, arquivos menores). Depois: fonte deduplicada e pequena (broadcast), Liquid
# Clustering na chave, concorrência entre writers.
# </details>
#
# **4. O que são deletion vectors e qual o custo?**
# <details><summary>Resposta</summary>
# Bitmap por arquivo marcando linhas apagadas/atualizadas: a escrita não reescreve o Parquet (merge-on-read).
# Custo: a leitura aplica o bitmap; precisa de OPTIMIZE/purge periódico; leitores antigos não suportam.
# Aqui: para atualizar 3 linhas, `numTargetRowsCopied` caiu de ~186 mil (a tabela inteira) para 0.
# </details>
#
# **5. Explique SCD1, SCD2 e SCD3 com um exemplo.**
# <details><summary>Resposta</summary>
# Repo transferido de conta: SCD1 guarda só o dono atual; SCD2 guarda uma linha por dono com
# `valid_from`/`valid_to`/`is_current`; SCD3 guarda atual + anterior em colunas. Escolha de negócio: atribuir
# a atividade passada ao dono de hoje (1) ou ao de então (2).
# </details>
#
# **6. Como sua SCD2 lida com dado que chega fora de ordem?**
# <details><summary>Resposta</summary>
# Recalcula a linha do tempo das chaves afetadas (histórico ∪ novo, ordenado por data efetiva, colapsando
# repetições) e aplica a diferença com MERGE (insert/update/delete). Demonstrado com a hora 11 chegando
# depois das 12–14 e num caso sintético de mudança no meio.
# </details>
#
# **7. Surrogate key: sequencial, identity ou hash?**
# <details><summary>Resposta</summary>
# Sequencial/identity é compacta mas não determinística (rebuild muda as chaves). Hash de (chave natural,
# valid_from) é estável e paralelizável; risco de colisão em 64 bits é desprezível no volume típico.
# </details>
#
# **8. `foreachBatch` é exactly-once?**
# <details><summary>Resposta</summary>
# Não por si só — é at-least-once (o lote pode rodar de novo após falha). Fica efetivamente exactly-once se
# a escrita for idempotente: MERGE por chave, ou `txnAppId`/`txnVersion` no writer Delta.
# </details>
#
# **9. Por que a condição do MERGE tem a data como literal se já casa por `event_id`?**
# <details><summary>Resposta</summary>
# Predicado só do alvo, com valor conhecido, permite podar partições/arquivos na fase de busca. Igualdade
# com a coluna da fonte (`t.event_date = s.event_date`) não poda nada no Delta OSS.
# </details>
#
# **10. Quando NÃO usar SCD2?**
# <details><summary>Resposta</summary>
# Atributo volátil (muda todo dia) — vira fato/snapshot; ninguém consulta o passado — SCD1 basta; histórico
# curto para auditoria — time travel do Delta (com retenção) pode resolver.
# </details>

# %% [markdown]
# ## Resumo
#
# - Silver = tipada, achatada, deduplicada; `from_json` com schema parcial (1 parse) e payload bruto preservado na bronze.
# - Dedup determinística com `row_number` + desempate; `dropDuplicates` não garante qual linha fica.
# - Stream Delta + `foreachBatch` + MERGE condicionado a hash = incremental **e** idempotente (provado: reprocessar não muda nada nem gera versão).
# - MERGE lento: predicado podável na condição + dado agrupado; deletion vectors eliminam a cópia de arquivo inteiro.
# - SCD2 genérica que recalcula a linha do tempo das chaves afetadas → aguenta dado atrasado; sk = hash(chave, valid_from) → idempotente.

# %%
spark.stop()
