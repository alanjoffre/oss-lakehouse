# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 15 · Observabilidade e custo
#
# > Este notebook prova que dá para responder "o que rodou, quanto demorou, o que falhou, o dado está
# > atualizado e quanto custou" com SQL — sem abrir a Spark UI e sem depender da memória de ninguém.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Arquitetura e desenvolvimento de pipelines | §2–§3 (registro de execução por etapa), §8 (freshness, volume, schema, distribuição, lineage), §9 (SLI/SLO/SLA) |
# | Databricks e processamento de dados | §5 (Spark UI REST), §6 (`DESCRIBE HISTORY`), §7 (`StreamingQueryListener`), ☁️ system tables e alertas SQL |
# | Microsoft Azure | ☁️ Azure Monitor / Log Analytics, Spot VMs, tags e chargeback |
# | Python avançado | §2 (context manager + decorator em `oss_lakehouse.observability`), §4 (logging estruturado) |
# | Times ágeis multidisciplinares | §8 (para onde vai o alerta e quem acorda), §10 (FinOps: custo por time) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup
#
# Duas áreas minhas neste notebook: `data/ops/pipeline_runs` (a tabela operacional com uma linha por etapa
# executada) e `data/demo/15/` (as tabelas que o pipeline de exemplo grava). As duas são **apagadas no início**
# para o notebook ser reprodutível — em produção a tabela de execuções só cresce. A bronze é só lida.

# %%
import io
import json
import logging
import shutil
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import functions as F

from oss_lakehouse.bronze import GH_EVENT_SCHEMA, read_gharchive_stream
from oss_lakehouse.config import get_settings
from oss_lakehouse.observability import (
    DeltaRunSink,
    ProgressCollector,
    check_freshness,
    check_volume,
    estimate_cost,
    last_commit_metrics,
    new_run_id,
    ops_path,
    raise_alerts,
    track_step,
    tracked,
)
from oss_lakehouse.perf import SparkUI, as_table
from oss_lakehouse.spark import get_spark
from oss_lakehouse.utils.logs import json_logger

settings = get_settings()
DEMO = Path(settings.data_root) / "demo" / "15"
RUNS_PATH = ops_path("pipeline_runs")
shutil.rmtree(DEMO, ignore_errors=True)
shutil.rmtree(RUNS_PATH, ignore_errors=True)
DEMO.mkdir(parents=True, exist_ok=True)

spark = get_spark("15")

# A hora do evento vira STRING 'yyyy-MM-dd HH:00' calculada no Spark (sessão em UTC): evita o erro
# clássico de trazer um timestamp para o Python e ele voltar no fuso da máquina.
bronze_raw = spark.read.format("delta").load(settings.path("bronze", "gh_events"))
bronze = bronze_raw.withColumn("event_ts", F.to_timestamp("created_at")).withColumn(
    "event_hour", F.date_format("event_ts", "yyyy-MM-dd HH:00")
)
por_hora = {r["event_hour"]: r["count"] for r in bronze.groupBy("event_hour").count().collect()}
HORAS = sorted(por_hora)[-3:]  # o pipeline de exemplo processa as 3 últimas horas, uma por execução
print(f"bronze: {sum(por_hora.values()):,} eventos em {len(por_hora)} hora(s)")
for h in HORAS:
    print(f"  {h}  {por_hora[h]:>9,} eventos")

# %% [markdown]
# ## 1. Os três pilares, aplicados a dados
#
# **O que é** — **Observabilidade** (*observability*) é conseguir explicar o estado interno de um sistema só
# pelo que ele emite. Em software, os três sinais clássicos são **métricas** (números agregados no tempo),
# **logs** (eventos discretos com contexto) e **traces** (*rastros*: o caminho de UMA requisição pelos
# componentes). Em dados existe um quarto, que software não tem: **lineage** (*linhagem*: de quais tabelas e
# arquivos este dado veio e quem o consome).
#
# **Por que importa** — Pipeline de dados tem um modo de falha que API não tem: **o job fica verde e o dado
# sai errado** (a fonte mandou metade, o schema mudou, o join duplicou). Monitorar só "o job terminou" não pega
# isso. É preciso observar a **execução** e o **dado**.
#
# **Como funciona** — o mapa deste notebook:
#
# | Sinal | Em software | Em dados (aqui) | Seção |
# |---|---|---|---|
# | Métricas | latência, taxa de erro, throughput | duração por etapa, taxa de falha, linhas lidas/escritas, linhas/s do stream | §3, §5, §7 |
# | Logs | linha de log com contexto | log estruturado (JSON) com `run_id`; histórico de commits do Delta | §4, §6 |
# | Traces | trace = requisição, span = chamada | `run_id` = execução do pipeline, etapa = span, `job_group` → stages na Spark UI | §2, §3, §5 |
# | Lineage | (não existe) | `_source_file` por linha; ☁️ `system.access.table_lineage` | §8 |
# | Qualidade do dado | (não existe) | freshness, volume, schema, distribuição | §8 |
#
# > 🎤 **Resposta de 30 s:** "Eu separo observabilidade de pipeline em duas perguntas. 'O job rodou bem?' —
# > respondo com uma tabela de execuções por etapa, logs estruturados com o id da execução e as métricas do
# > Spark. 'O dado está bom?' — respondo com freshness, volume, schema e distribuição, mais lineage para saber o
# > impacto. O incidente caro é o job verde com dado errado, então a segunda pergunta precisa de alerta próprio."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Monitoramento × observabilidade:** monitorar é vigiar o que você já sabia que podia quebrar (limite em
#   métrica conhecida); observabilidade é ter contexto suficiente para investigar o que você **não** previu
#   (por isso alta cardinalidade: `run_id`, tabela, partição, arquivo).
# - **Qualidade × observabilidade de dados:** qualidade (notebook 08) é regra explícita e determinística que
#   barra o dado (*expectation*); observabilidade é detecção — muitas vezes estatística — de anomalia no que
#   passou pela regra. As duas se complementam: regra para o que eu sei, anomalia para o que eu não sei.
# - **OpenTelemetry:** padrão aberto para métricas, logs e traces. Em pipelines Spark o uso mais comum é
#   **OpenLineage** (padrão aberto de eventos de lineage por execução), que tem integração com Spark e Airflow.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Observabilidade custa: armazenamento, tempo de execução (cada `count()` é um scan) e atenção humana.
#   Comece pelas tabelas que alguém usa para decidir, não por todas.
# - Alerta sem dono e sem ação definida é ruído. Menos alertas, cada um com *runbook* (roteiro de resposta).

# %% [markdown]
# ## 2. Registro de execução por etapa 🧪
#
# **O que é** — Uma tabela Delta, `ops/pipeline_runs`, com **uma linha por etapa executada**: pipeline, etapa,
# `run_id` (id da execução do pipeline, compartilhado pelas etapas), início, fim, duração, linhas lidas e
# escritas, status e erro. Quem grava é `track_step`, um **context manager** (objeto usado com `with`) do
# pacote; `tracked` é a versão **decorator**.
#
# **Por que importa** — A Spark UI some quando o cluster desliga e o log do driver é texto solto. Uma tabela
# sobrevive, cruza com outras e responde com SQL: "qual etapa ficou mais lenta esta semana?".
#
# **Como funciona** — o registro acontece no `finally`: **a etapa que falha também é registrada, e a exceção
# continua subindo**. Observar não pode engolir erro (senão o orquestrador marcaria sucesso) nem esconder o erro
# original (se a gravação do registro falhar, isso vai para o log e o erro da etapa segue).
#
# ```text
# with track_step("gh_hourly", "agrega_por_tipo", sink, spark, run_id=rid) as run:
#     ...trabalho...            run.rows_read = n ; run.rows_written = m
# └─ sucesso → status=SUCCESS ─┐
# └─ exceção → status=FAILED, error="Tipo: 1ª linha" ─┤→ finally: fim, duração, sink.write(run) → raise
# ```
#
# O pipeline de exemplo (`gh_hourly`) processa **uma hora** da bronze por execução, em quatro etapas reais.
# Cada gravação usa `replaceWhere` na hora processada: reprocessar a mesma hora **substitui** as linhas dela
# (idempotência), então um *retry* (nova tentativa) é seguro.

# %%
sink = DeltaRunSink(spark, RUNS_PATH)
PIPELINE = "gh_hourly"
OUT_TIPO = str(DEMO / "eventos_por_tipo_hora")
OUT_REPOS = str(DEMO / "top_repos_hora")
OUT_ORGS = str(DEMO / "top_orgs_hora")


def grava_hora(df, path: str, hora: str) -> int:
    """Sobrescreve só as linhas da hora (idempotente) e devolve as linhas gravadas, lidas do log do Delta."""
    df.write.format("delta").mode("overwrite").option("replaceWhere", f"event_hour = '{hora}'").save(path)
    return int(last_commit_metrics(spark, path)["numOutputRows"])  # sem pagar um count() a mais


def executa_pipeline(hora: str, com_bug: bool = False) -> str:
    """Uma execução do pipeline para `hora`. Devolve o run_id. `com_bug` simula um deploy com erro."""
    rid = new_run_id()
    eventos = bronze.where(F.col("event_hour") == hora)

    with track_step(PIPELINE, "valida_entrada", sink, spark, run_id=rid) as run:
        linha = eventos.agg(F.count("*").alias("n"), F.sum(F.col("id").isNull().cast("int")).alias("sem_id")).first()
        run.rows_read = n = linha["n"]
        if n == 0 or linha["sem_id"]:
            raise ValueError(f"entrada inválida para {hora}: {n} eventos, {linha['sem_id']} sem id")

    with track_step(PIPELINE, "agrega_por_tipo", sink, spark, run_id=rid) as run:
        agg = eventos.groupBy("event_hour", "type").agg(
            F.count("*").alias("eventos"), F.approx_count_distinct("actor.login").alias("atores")
        )
        run.rows_read = n
        run.rows_written = grava_hora(agg, OUT_TIPO, hora)

    # A mesma coisa como decorator: a função devolve as contagens e `tracked` as registra.
    @tracked(PIPELINE, "top_repos", sink, spark, run_id=rid)
    def top_repos() -> dict[str, int]:
        top = eventos.groupBy("event_hour", F.col("repo.name").alias("repo")).count().orderBy(F.desc("count")).limit(100)
        return {"rows_read": n, "rows_written": grava_hora(top, OUT_REPOS, hora)}

    top_repos()

    with track_step(PIPELINE, "top_orgs", sink, spark, run_id=rid) as run:
        campo = "org.nome" if com_bug else "org.login"  # o "bug": campo que não existe no struct
        top = (
            eventos.where(F.col("org").isNotNull())
            .groupBy("event_hour", F.col(campo).alias("org"))
            .count()
            .orderBy(F.desc("count"))
            .limit(100)
        )
        run.rows_read = n
        run.rows_written = grava_hora(top, OUT_ORGS, hora)
    return rid


# %% [markdown]
# Quatro execuções: as três horas, com a do meio **falhando** na última etapa (o "deploy com bug"), e depois o
# **retry** da hora que falhou, já com a correção. A exceção da etapa chega até aqui — é este `try/except` do
# notebook que a segura, não o `track_step`.

# %%
rid_falha = None
for i, hora in enumerate(HORAS):
    bug = i == 1
    t0 = time.perf_counter()
    try:
        rid = executa_pipeline(hora, com_bug=bug)
        print(f"{hora}  run {rid[:8]}  OK      {time.perf_counter() - t0:5.1f}s")
    except Exception as exc:  # noqa: BLE001 (demonstração: mostrar que a exceção propagou)
        rid_falha = spark.read.format("delta").load(RUNS_PATH).where("status = 'FAILED'").first()["run_id"]
        print(f"{hora}  run {rid_falha[:8]}  FALHOU  {time.perf_counter() - t0:5.1f}s")
        print(f"   exceção que propagou: {type(exc).__name__}: {str(exc).splitlines()[0][:110]}")

t0 = time.perf_counter()
rid_retry = executa_pipeline(HORAS[1])
print(f"{HORAS[1]}  run {rid_retry[:8]}  OK      {time.perf_counter() - t0:5.1f}s   (retry depois da correção)")

# %% [markdown]
# A tabela de execuções, como ficou. A linha `FAILED` tem o tipo e a primeira linha do erro; as etapas
# anteriores da mesma execução (`run_id` igual) estão como `SUCCESS` — exatamente o que aconteceu.

# %%
runs = spark.read.format("delta").load(RUNS_PATH)
runs.createOrReplaceTempView("pipeline_runs")
spark.sql("""
    SELECT substr(run_id, 1, 8) AS run, step, date_format(started_at, 'HH:mm:ss') AS inicio_utc,
           duration_s, rows_read, rows_written, status, substr(error, 1, 48) AS erro
    FROM pipeline_runs ORDER BY started_at
""").show(20, truncate=False)

# %% [markdown]
# A prova de idempotência do retry: a hora que falhou e foi reprocessada aparece **uma vez** em cada tabela de
# saída, com o mesmo número de linhas das outras execuções — `replaceWhere` trocou, não somou.

# %%
for nome, path in [("eventos_por_tipo_hora", OUT_TIPO), ("top_repos_hora", OUT_REPOS), ("top_orgs_hora", OUT_ORGS)]:
    linhas = spark.read.format("delta").load(path).groupBy("event_hour").count().orderBy("event_hour").collect()
    print(f"{nome:<22}", "  ".join(f"{r['event_hour'][-5:]}→{r['count']}" for r in linhas))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Cada etapa do pipeline roda dentro de um context manager que grava uma linha
# > numa tabela Delta de execuções: pipeline, etapa, id da execução, início, fim, duração, linhas lidas e
# > escritas, status e erro. O registro fica no `finally`, então a falha também é registrada e a exceção
# > continua subindo para o orquestrador. Linhas escritas eu leio do `operationMetrics` do commit do Delta, sem
# > um `count()` extra. Com isso, duração por etapa, taxa de falha e tendência viram consultas SQL."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que `except BaseException`:** `KeyboardInterrupt` e o cancelamento do job não herdam de `Exception`.
#   Etapa cancelada também precisa ficar registrada como não concluída.
# - **Fuso:** o PySpark trata `datetime` **sem** fuso como hora local da máquina. Esta máquina está em −03:00;
#   o módulo grava datetimes com fuso (UTC) e há um teste que fixa `TZ=America/Sao_Paulo` para garantir.
# - **Arquivos pequenos:** um `append` por etapa = um commit e um arquivo Parquet por linha. Mostro o efeito e o
#   `OPTIMIZE` no §6. Alternativas: acumular e gravar uma vez no fim do job, ou mandar para um sistema de
#   métricas e materializar a tabela depois.
# - **Etapa `RUNNING` que nunca termina:** aqui a linha só é gravada no fim; se o driver morrer (OOM, spot
#   removido), não há linha. Quem cobre isso é o orquestrador (no Databricks, `system.lakeflow.job_run_timeline`)
#   e um alerta de **ausência**: "não há execução com sucesso há mais de X horas" (§8, freshness).
# - **Concorrência:** `append` no Delta não conflita com outro `append` (isolamento otimista só briga quando
#   alguém lê e reescreve os mesmos arquivos), então vários jobs podem gravar na mesma tabela de execuções.
# - **No Databricks não reinvente o que já existe:** duração e resultado por job/task já estão nas system
#   tables. A tabela própria vale pelo que a plataforma não sabe: linhas lidas/escritas, a partição processada,
#   o motivo de negócio da falha.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `rows_read` com `count()` é um scan a mais. Aqui a contagem já fazia parte da validação de entrada; quando
#   não fizer, prefira métricas que o motor já calcula (`operationMetrics`, `numInputRows` do stream).
# - Tabela caseira precisa de dono, retenção e compactação. Para um pipeline só, o histórico do job basta.

# %% [markdown]
# ## 3. Consultas sobre as execuções: duração, taxa de falha, tendência 🧪
#
# **O que é** — As três perguntas operacionais básicas, respondidas com SQL sobre `pipeline_runs`.
#
# **Por que importa** — "Está mais lento" e "falha muito" precisam de número e de linha de base para virar
# decisão (otimizar? aumentar cluster? rever o SLA?).
#
# **Como funciona** — agregação por etapa para duração e falha; *window function* (função de janela) para
# comparar cada execução com a mediana das **anteriores** da mesma etapa.

# %%
print("Duração e taxa de falha por etapa")
spark.sql("""
    SELECT step,
           count(*)                                              AS execucoes,
           sum(CAST(status = 'FAILED' AS INT))                   AS falhas,
           round(100 * avg(CAST(status = 'FAILED' AS INT)), 1)   AS pct_falha,
           round(percentile_approx(duration_s, 0.5), 2)          AS mediana_s,
           round(max(duration_s), 2)                             AS max_s,
           round(sum(duration_s), 2)                             AS total_s
    FROM pipeline_runs GROUP BY step ORDER BY total_s DESC
""").show(truncate=False)

print("Por execução do pipeline (uma execução só é boa se TODAS as etapas passaram)")
spark.sql("""
    SELECT substr(run_id, 1, 8) AS run, date_format(min(started_at), 'HH:mm:ss') AS inicio_utc,
           count(*) AS etapas, max(rows_read) AS linhas_lidas,
           round(sum(duration_s), 1) AS soma_etapas_s,
           round((unix_millis(max(finished_at)) - unix_millis(min(started_at))) / 1000, 1) AS parede_s,
           round((unix_millis(max(finished_at)) - unix_millis(min(started_at))) / 1000 - sum(duration_s), 1)
               AS entre_etapas_s,
           CASE WHEN max(CAST(status = 'FAILED' AS INT)) = 1 THEN 'FAILED' ELSE 'SUCCESS' END AS status
    FROM pipeline_runs GROUP BY run_id ORDER BY min(started_at)
""").show(truncate=False)

# %% [markdown]
# **O custo de observar, medido.** `parede_s` vai do início da primeira etapa ao fim da última;
# `soma_etapas_s` é só o trabalho das etapas. A diferença (`entre_etapas_s`) é o tempo gasto **entre** as etapas
# — e entre elas só existe uma coisa: gravar a linha de registro (montar um DataFrame de uma linha e fazer um
# commit Delta). Nesta máquina, com tabelas minúsculas, esse tempo é da mesma ordem do trabalho útil. É um
# resultado local exagerado (etapas de poucos segundos, laptop compartilhado, e cada commit Delta relê o log da
# tabela — ver §5), mas a lição vale em qualquer escala: **um commit por etapa tem custo fixo**. Em produção,
# com etapas de minutos, ele some na conta; com muitas etapas curtas, acumule os registros e grave uma vez no
# fim do job.

# %% [markdown]
# **Tendência.** Para cada execução com sucesso, a razão entre a duração dela e a mediana das execuções
# anteriores da mesma etapa. Razão muito acima de 1 por várias execuções seguidas = regressão; a primeira linha
# de cada etapa não tem linha de base (`NULL`).

# %%
tendencia = spark.sql("""
    SELECT step, date_format(started_at, 'HH:mm:ss') AS inicio_utc, duration_s, rows_read,
           round(percentile_approx(duration_s, 0.5) OVER w, 2)                AS mediana_anteriores_s,
           round(duration_s / percentile_approx(duration_s, 0.5) OVER w, 2)  AS razao
    FROM pipeline_runs
    WHERE status = 'SUCCESS'
    WINDOW w AS (PARTITION BY step ORDER BY started_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    ORDER BY step, started_at
""")
tendencia.show(20, truncate=False)

# %% [markdown]
# **Como ler a saída acima — e o que ela não prova.** Dois efeitos aparecem, e nenhum é tendência:
#
# - **Cold start** (*partida a frio*): em `valida_entrada`, a primeira execução custa várias vezes as
#   seguintes (a segunda linha tem razão muito abaixo de 1). É a primeira consulta da sessão: paga a geração e
#   a compilação de código pela JVM e a primeira leitura do log do Delta. Não é o pipeline "melhorando".
# - **Ruído:** nas outras etapas a razão oscila em torno de 1, para cima e para baixo — às vezes com um pico
#   isolado bem acima —, com o volume praticamente igual (coluna `rows_read`). É a variação de uma máquina
#   compartilhada, não uma regressão: por isso o alerta exige persistência, e não uma execução só.
#
# Duas lições práticas:
#
# 1. Compare execuções **comparáveis** (mesmo tipo de cluster, mesma faixa de volume, descartando a partida a
#    frio); normalize por linha (`duration_s / rows_read`) quando o volume varia.
# 2. Com 3–4 pontos não existe tendência, existe ruído. Em produção a janela é de dias ou semanas, e o alerta
#    exige persistência (por exemplo, três execuções seguidas acima de 1,5× a mediana).
#
# **O "trace" de uma execução:** todas as etapas de um `run_id`, em ordem — é a visão que se abre primeiro num
# incidente. Abaixo, a execução que falhou.

# %%
spark.sql(f"""
    SELECT step, date_format(started_at, 'HH:mm:ss.SSS') AS inicio_utc, duration_s, status, job_group,
           substr(error, 1, 60) AS erro
    FROM pipeline_runs WHERE run_id = '{rid_falha}' ORDER BY started_at
""").show(truncate=False)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Com a tabela de execuções eu respondo três coisas: duração por etapa com mediana
# > e máximo, taxa de falha por etapa e por execução, e tendência com window function comparando cada execução
# > à mediana das anteriores. Cuidado com duas armadilhas: cold start na primeira execução e variação de volume
# > — normalizo por linha e só alerto se o desvio persistir."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Mediana e percentis, não média:** duração tem cauda longa; uma execução de 40 min puxa a média e esconde
#   que o típico são 5. Para SLA, olha-se p95/p99.
# - **Taxa de falha por etapa × por execução:** uma etapa com 25% de falha num pipeline de quatro etapas
#   derruba 25% das execuções. O usuário sente a taxa por execução.
# - **Retry mascara falha:** se o orquestrador tenta de novo e passa, o job fica verde. A tabela guarda as duas
#   tentativas; "taxa de falha na primeira tentativa" é um indicador de saúde melhor que o status final.
# - **Detecção de anomalia de verdade:** sazonalidade (segunda ≠ domingo) pede linha de base por dia da semana
#   e hora, ou um modelo; régua fixa é o começo, não o fim.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Limite fixo (ex.: "mais de 10 min") é simples e explica bem; envelhece mal quando o volume cresce.
# - Limite relativo à mediana se adapta sozinho — inclusive a uma degradação lenta, que ele "aprende" e deixa
#   de apontar. Use os dois: relativo para detectar, fixo (o SLA) como teto.

# %% [markdown]
# ## 4. Logging estruturado 🧪
#
# **O que é** — **Log estruturado** é o log em que cada linha é um registro com campos (JSON), e não uma frase.
# Em vez de `"etapa top_orgs falhou em 0.2s"`, grava-se `{"step": "top_orgs", "status": "FAILED", ...}`.
#
# **Por que importa** — Ferramenta de log (Log Analytics, Datadog, Elastic) filtra e agrega por **campo**.
# Com texto livre, cada pergunta vira uma expressão regular frágil. O campo mais valioso é o **id de
# correlação** — aqui o `run_id` — que junta as linhas de uma mesma execução.
#
# **Como funciona** — `oss_lakehouse.utils.logs.json_logger` instala um `Formatter` que serializa o registro e
# tudo que vier em `extra=`. Abaixo, as etapas da execução que falhou viram linhas de log; depois eu filtro por
# campo, como uma ferramenta de log faria.

# %%
buffer = io.StringIO()
logger = json_logger("gh_hourly", stream=buffer)
for r in runs.where(F.col("run_id") == rid_falha).orderBy("started_at").collect():
    logger.log(
        logging.ERROR if r["status"] == "FAILED" else logging.INFO,
        "etapa finalizada",
        extra={
            "run_id": r["run_id"][:8],
            "pipeline": r["pipeline"],
            "step": r["step"],
            "status": r["status"],
            "duration_s": r["duration_s"],
            "rows_written": r["rows_written"],
            "error": (r["error"] or "")[:60] or None,
        },
    )
linhas_log = buffer.getvalue().splitlines()
for linha in linhas_log:
    print(linha[:230])

erros = [json.loads(linha) for linha in linhas_log if json.loads(linha)["level"] == "ERROR"]
print(f"\nfiltro por campo (level=ERROR): {len(erros)} de {len(linhas_log)} linhas → step={erros[0]['step']}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Log em JSON, uma linha por evento, com campos fixos: timestamp em UTC, nível,
# > pipeline, etapa e um id de correlação da execução. Assim a ferramenta de log filtra por campo e eu junto log,
# > tabela de execuções e Spark UI pelo mesmo id. Nunca dado pessoal nem segredo no log."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Onde o log vai parar no Databricks:** o `stdout`/`stderr`/log4j do driver ficam no cluster e somem com
#   ele, a menos que se configure *compute log delivery* (entrega de logs do cluster para um destino, ex.: um
#   Volume) ou se envie para o Azure Monitor. Em serverless não há acesso ao log4j do driver — mais um motivo
#   para o que importa ir para tabela.
# - **Log de executor:** `print` dentro de UDF roda no executor e não aparece no notebook. Para contar eventos
#   dentro de uma transformação, a ferramenta é métrica (acumulador, métrica observável com `df.observe`).
# - **Nível:** `INFO` para marcos do pipeline, `WARNING` para o que degradou mas seguiu, `ERROR` para o que
#   exige ação. Log por linha de dado é proibitivo em volume e em risco de vazar dado.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - JSON é ruim de ler a olho no terminal; em desenvolvimento local um formatter de texto é aceitável — o que
#   não pode é o formato de produção depender de quem está olhando.
# - Log não substitui a tabela de execuções: log é para investigar um caso, tabela é para agregar muitos.

# %% [markdown]
# ## 5. Métricas da Spark UI pela REST API 🧪
#
# **O que é** — A Spark UI expõe em `<uiWebUrl>/api/v1` os mesmos dados das abas Jobs/Stages/SQL: tempo de
# execução, bytes lidos, **shuffle** (redistribuição de dados entre partições) e **spill** (quando a memória
# não basta e a operação derrama para o disco).
#
# **Por que importa** — A tabela de execuções diz **que** a etapa demorou; as métricas dos stages dizem **por
# quê** (leu demais? shuffle grande? spill?).
#
# **Como funciona** — `track_step` marcou os jobs de cada etapa com um **job group** (rótulo), gravado na
# coluna `job_group`. É a ponte entre a linha da tabela e os stages daquela etapa: o "trace" desce um nível.
# O cliente `SparkUI` é do módulo `oss_lakehouse.perf` (notebook 09).

# %%
ui = SparkUI.of(spark)
alvo = spark.sql("""
    SELECT job_group, duration_s FROM pipeline_runs
    WHERE step = 'agrega_por_tipo' AND status = 'SUCCESS' ORDER BY started_at DESC LIMIT 1
""").first()
jobs_etapa = ui.jobs(alvo["job_group"])
stages = sorted(ui.stage_summaries(alvo["job_group"]), key=lambda s: -s.run_time_ms)
print(f"etapa {alvo['job_group']}: {alvo['duration_s']}s de parede, {len(jobs_etapa)} jobs, {len(stages)} stages")
print("os 5 stages com mais tempo de execução:")
print(as_table([s.as_row() for s in stages[:5]]))

# %% [markdown]
# Leitura — e ela contraria a intuição:
#
# - Uma etapa "simples" (um `groupBy` e uma gravação) disparou **mais de dez** jobs. A agregação é uma parte;
#   o resto são jobs do próprio Delta.
# - Os stages com mais tempo **não são os do dado**: têm nome `recordDeltaOperationInternal` e **50 tasks** —
#   é o Delta reconstruindo o estado da tabela a partir do log de transações (*log replay*), que por padrão
#   usa 50 partições (`spark.databricks.delta.snapshotPartitions`). Os stages que leem o dado aparecem com
#   poucas tasks e fração de MB de entrada (Parquet com poda de colunas).
# - `tempo_exec_s` é a **soma** do tempo das tasks, que rodam em paralelo: pode passar do tempo de parede.
# - Sem spill e sem shuffle relevante nos stages listados.
#
# Conclusão honesta: nesta escala, a etapa é dominada por **custo fixo do Delta**, não pelo volume. É o mesmo
# custo fixo que apareceu no §3 entre as etapas. Em tabela pequena com muitos commits, esse é o primeiro lugar
# para olhar (menos commits, log compactado por checkpoint, menos partições de snapshot em ambiente local).
#
# > 🎤 **Resposta de 30 s:** "Eu marco os jobs de cada etapa com um job group e guardo o rótulo na tabela de
# > execuções. Quando uma etapa fica lenta, pego o rótulo e leio os stages pela REST API da Spark UI: tempo,
# > entrada, shuffle e spill. No Databricks, para histórico, uso `system.query.history` e a Spark UI do job run
# > — a UI ao vivo some com o cluster."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Retenção:** a UI guarda um número limitado de jobs/stages em memória (`spark.ui.retainedJobs`,
#   `retainedStages`); para depois do fim da aplicação existe o *event log* + History Server. No Databricks a
#   UI de clusters encerrados fica disponível por tempo limitado.
# - **Métricas contínuas:** o Spark tem um sistema de métricas (Dropwizard) com *sinks* (destinos) para
#   Prometheus, JMX, Graphite — é o caminho para painel de CPU/GC/memória ao vivo.
# - **Serverless e Spark Connect:** não há `sparkContext` nem `uiWebUrl`. Usa-se o *query profile* e as system
#   tables.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Ler a REST API a cada etapa em produção acopla o pipeline a uma API de diagnóstico. Use sob demanda (como
#   aqui) ou num listener; o registro de rotina fica com as métricas baratas.

# %% [markdown]
# ## 6. `DESCRIBE HISTORY`: a trilha de auditoria que já vem pronta 🧪
#
# **O que é** — Cada commit de uma tabela Delta fica registrado no log de transações com **quem**, **quando**,
# **qual operação**, com quais parâmetros e com quais métricas (`operationMetrics`). `DESCRIBE HISTORY` lê isso.
#
# **Por que importa** — É auditoria sem código: "quem sobrescreveu esta tabela ontem?", "quantas linhas o
# MERGE das 3h atualizou?", "desde quando este arquivo existe?". E é a base do *time travel* (notebook 05).
#
# **Como funciona** — abaixo, o histórico da tabela `eventos_por_tipo_hora`. Foram quatro gravações (três
# horas + o retry). Repare no predicado do `replaceWhere` e nas linhas gravadas: o retry da hora do meio é a
# última versão, com o mesmo predicado da versão 1 e **um arquivo removido** — o arquivo antigo daquela hora
# saiu, o novo entrou, no mesmo commit (troca atômica).

# %%
hist = spark.sql(f"DESCRIBE HISTORY delta.`{OUT_TIPO}`")
hist.select(
    "version",
    F.date_format("timestamp", "HH:mm:ss").alias("hora"),
    "operation",
    F.col("operationParameters.predicate").alias("predicado"),
    F.col("operationMetrics.numOutputRows").alias("linhas"),
    F.col("operationMetrics.numFiles").alias("arquivos"),
    F.col("operationMetrics.numRemovedFiles").alias("arq_removidos"),
    "userName",
).orderBy("version").show(truncate=False)

# %% [markdown]
# `userName` vem `NULL` no Delta local (não há identidade); no Databricks vêm preenchidos `userName`, `job`
# (id do job e do run), `notebook` e `clusterId` — é isso que torna o histórico uma trilha de auditoria.
#
# O mesmo histórico mostra o **custo do registro etapa a etapa** (§2): a tabela de execuções recebeu um commit
# e um arquivo por linha. `OPTIMIZE` junta os arquivos pequenos — e a própria compactação fica auditada.

# %%
detalhe = spark.sql(f"DESCRIBE DETAIL delta.`{RUNS_PATH}`").first()
n_linhas = runs.count()
print(f"pipeline_runs antes : {n_linhas} linhas em {detalhe['numFiles']} arquivos ({detalhe['sizeInBytes']:,} bytes)")

m = spark.sql(f"OPTIMIZE delta.`{RUNS_PATH}`").first()["metrics"]
detalhe = spark.sql(f"DESCRIBE DETAIL delta.`{RUNS_PATH}`").first()
print(f"OPTIMIZE            : removeu {m['numFilesRemoved']} arquivos, criou {m['numFilesAdded']}")
print(f"pipeline_runs depois: {runs.count()} linhas em {detalhe['numFiles']} arquivo(s) ({detalhe['sizeInBytes']:,} bytes)")

spark.sql(f"DESCRIBE HISTORY delta.`{RUNS_PATH}`").groupBy("operation").agg(
    F.count("*").alias("commits"), F.min("version").alias("da_versao"), F.max("version").alias("ate_versao")
).show()

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Toda escrita em Delta deixa um commit com usuário, operação, parâmetros e
# > métricas. `DESCRIBE HISTORY` me dá a trilha de auditoria da tabela e, pelo `operationMetrics`, linhas
# > inseridas, atualizadas e removidas sem eu instrumentar nada. Para auditoria de **acesso** — quem leu — o
# > histórico não serve; aí é `system.access.audit` no Unity Catalog."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Retenção:** o histórico vive no log e é limpo conforme `delta.logRetentionDuration` (padrão 30 dias).
#   Auditoria de longo prazo exige copiar o histórico para outra tabela ou usar as system tables.
# - **Só escrita:** leitura não gera commit. Quem consultou a tabela está no log de auditoria da plataforma.
# - **`operationMetrics` por operação:** `WRITE` traz `numOutputRows`/`numFiles`; `MERGE` traz
#   `numTargetRowsInserted/Updated/Deleted` e tempos de scan/rewrite; `OPTIMIZE` traz arquivos removidos e
#   adicionados. Um MERGE que de repente atualiza 100× mais linhas é um sinal de anomalia de graça.
# - **Os arquivos removidos continuam no disco** até o `VACUUM` (é o que permite o time travel). O `OPTIMIZE`
#   acima reduziu os arquivos **ativos**, não o espaço ocupado.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Histórico não é sistema de monitoramento: não alerta sozinho. É fonte para as checagens do §8.
# - Não conte com ele para requisito legal de retenção: `VACUUM` e a limpeza do log apagam o passado.

# %% [markdown]
# ## 7. Streaming: `StreamingQueryListener` 🧪
#
# **O que é** — Um stream do Structured Streaming processa o dado em **micro-lotes** (*micro-batches*). A cada
# lote o Spark emite um evento de progresso: linhas de entrada, linhas/s recebidas e processadas, duração de
# cada fase, estado, watermark. `StreamingQueryListener` é a interface para **receber** esses eventos.
#
# **Por que importa** — Stream não "termina": não existe status final para olhar. A saúde de um stream é uma
# série temporal — está acompanhando a fonte ou ficando para trás? O lote está demorando mais que o intervalo?
#
# **Como funciona** — `ProgressCollector` (do pacote) guarda cada progresso numa lista. Aqui o stream lê os
# arquivos de `data/landing/gharchive` com o schema explícito da bronze (`read_gharchive_stream`), **um arquivo
# por lote**, em modo `availableNow` (processa o que existe e para), gravando num destino e num checkpoint
# próprios deste notebook.

# %%
collector = ProgressCollector()
spark.streams.addListener(collector)
STREAM_OUT = str(DEMO / "stream_events")
try:
    stream = read_gharchive_stream(spark, settings.path("landing", "gharchive"), max_files_per_trigger=1).select(
        "id", "type", "created_at", F.col("_metadata.file_name").alias("_source_file")
    )
    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", str(DEMO / "_checkpoints" / "stream_events"))
        .trigger(availableNow=True)
        .queryName("15_stream_gh_events")
        .start(STREAM_OUT)
    )
    query.awaitTermination()
    for _ in range(80):  # o listener é chamado de forma assíncrona: espera o evento de término chegar
        if collector.terminated:
            break
        time.sleep(0.25)
finally:
    spark.streams.removeListener(collector)

print(as_table([{k: v for k, v in p.items() if k != "query"} for p in collector.progress]))
total_listener = sum(p["num_input_rows"] for p in collector.progress)
total_destino = spark.read.format("delta").load(STREAM_OUT).count()
print(f"\nlotes com dado: {sum(1 for p in collector.progress if p['num_input_rows'])}"
      f" | linhas vistas pelo listener: {total_listener:,} | linhas no destino: {total_destino:,}"
      f" | término com erro: {collector.terminated[0]['exception']}")

# %% [markdown]
# O total visto pelo listener bate com o que foi gravado no destino: o progresso é uma fonte confiável de
# **volume** sem `count()`. Uma regra de alerta sobre esses eventos, no mesmo espírito das do §8:

# %%
lotes = [p for p in collector.progress if p["num_input_rows"]]
mais_lento = max(lotes, key=lambda p: p["trigger_ms"])
INTERVALO_ALVO_MS = 60_000  # exemplo: stream agendado para rodar a cada 1 min
print(f"lote mais lento: batch {mais_lento['batch_id']} com {mais_lento['trigger_ms']:,} ms"
      f" ({mais_lento['num_input_rows']:,} linhas, {mais_lento['processed_rows_per_s']:,.0f} linhas/s)")
print("regra: duração do lote > intervalo do gatilho →",
      "ALERTA (ficando para trás)" if mais_lento["trigger_ms"] > INTERVALO_ALVO_MS else "OK (cabe no intervalo)")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Stream não tem status final, então eu monitoro o progresso de cada micro-lote com
# > um `StreamingQueryListener`: linhas de entrada, linhas por segundo processadas contra recebidas e duração
# > do lote contra o intervalo do gatilho. Se o processado fica abaixo do recebido por vários lotes, o stream
# > está acumulando atraso — o alerta sai antes de o consumidor reclamar do dado velho."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`inputRowsPerSecond` × `processedRowsPerSecond`:** o primeiro é a taxa de chegada medida entre gatilhos;
#   o segundo é linhas ÷ duração do lote. Em `availableNow` o dado já estava todo lá, então a taxa de entrada
#   não representa uma fonte ao vivo — o indicador que vale aqui é a duração do lote.
# - **Atraso real (lag):** com Kafka/Event Hubs, compare o offset processado com o último offset da fonte (o
#   progresso traz `sources[].startOffset/endOffset/latestOffset`). No Auto Loader, o tamanho do backlog vem em
#   `sources[].metrics` (`numFilesOutstanding`, `numBytesOutstanding`).
# - **Outros campos úteis:** `stateOperators` (linhas em estado, memória — estado crescendo sem parar =
#   watermark errado), `eventTime.watermark`, `durationMs` por fase (`addBatch`, `walCommit`, `latestOffset`).
# - **O listener roda no driver, numa fila assíncrona:** código lento ou que lança exceção ali atrasa ou perde
#   eventos. Ele deve só enfileirar/enviar; nada de consulta pesada.
# - **Sem listener:** `query.lastProgress` e `query.recentProgress` dão o mesmo dado sob consulta.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Para um job `availableNow` agendado, a tabela de execuções (§2) costuma bastar; o listener paga o esforço
#   em stream contínuo.
# - No Databricks, Lakeflow Declarative Pipelines já grava esse progresso no *event log* do pipeline — lá,
#   consulte o event log em vez de manter um listener próprio.

# %% [markdown]
# ## 8. Data observability: freshness, volume, schema, distribuição, lineage 🧪
#
# **O que é** — **Data observability** é vigiar o **dado**, não o job. Os cinco sinais de mercado:
#
# | Sinal | Pergunta | Como medir |
# |---|---|---|
# | **Freshness** (*atualidade*) | O dado mais novo tem quanto tempo? | `now − max(timestamp do evento ou da carga)` |
# | **Volume** | Chegou a quantidade de sempre? | linhas da carga × linha de base histórica |
# | **Schema** | Entrou, saiu ou mudou coluna? | schema atual × schema esperado (contrato) |
# | **Distribuição** | Os valores mudaram de perfil? | % de nulos, proporção por categoria, min/max/média × histórico |
# | **Lineage** (*linhagem*) | De onde veio e quem é afetado? | grafo tabela→tabela; origem por linha |
#
# **Por que importa** — São os defeitos que passam por um job verde: a API mudou um campo, o parceiro mandou
# meio arquivo, um filtro novo cortou 30% das linhas.
#
# **Como funciona** — cada checagem é **regra + mensagem**: `check_freshness` e `check_volume` (do pacote)
# devolvem um `CheckResult` (ok?, observado, esperado) e `raise_alerts` envia as que falharam para um `notify`.
#
# ### 8.1 Freshness e volume, com alerta
#
# Dois cenários para cada checagem. Em freshness: o relógio **de verdade** (os dados são do dia 2026-10-01 e o
# notebook roda dias depois — o alerta dispara, e é o comportamento correto) e o relógio como seria logo depois
# da carga. Em volume: a última hora inteira e uma **carga parcial** (só os primeiros 20 minutos da hora,
# simulando um arquivo truncado).

# %%
ultimo_evento = datetime.fromisoformat(bronze.agg(F.max("created_at")).first()[0].replace("Z", "+00:00"))
agora = datetime.now(UTC)
SLA_FRESHNESS = timedelta(hours=2)

ultima_hora = HORAS[-1]
historico = [n for h, n in sorted(por_hora.items()) if h != ultima_hora]
carga_parcial = bronze.where((F.col("event_hour") == ultima_hora) & (F.minute("event_ts") < 20)).count()

checagens = [
    check_freshness(ultimo_evento, agora, SLA_FRESHNESS, name="freshness bronze.gh_events (relógio real)"),
    check_freshness(ultimo_evento, ultimo_evento + timedelta(minutes=40), SLA_FRESHNESS,
                    name="freshness bronze.gh_events (40 min após a carga)"),
    check_volume(por_hora[ultima_hora], historico, tolerance=0.5, name=f"volume {ultima_hora} (hora inteira)"),
    check_volume(carga_parcial, historico, tolerance=0.5, name=f"volume {ultima_hora} (carga parcial)"),
]
print(f"último evento: {ultimo_evento:%Y-%m-%d %H:%M:%S} UTC | histórico de volume: {len(historico)} hora(s)\n")
for c in checagens:
    print(c)

# %% [markdown]
# **Regra + mensagem + destino.** O alerta precisa dizer o que foi observado, o que era esperado, qual tabela,
# qual a severidade e o que fazer. O `notify` abaixo só monta e imprime a mensagem — **nada é enviado** (o
# notebook roda offline). A severidade decide o destino.

# %%
ROTAS = {
    "critico": "PagerDuty — abre incidente e aciona o plantão",
    "aviso": "canal do time no Teams — alguém olha no horário comercial",
    "info": "resumo diário por e-mail",
}


def severidade(nome: str) -> str:
    return "critico" if nome.startswith("freshness") else "aviso"


enviados: list[dict] = []


def notify(mensagem: str) -> None:
    nome = mensagem.split("] ", 1)[1].split(":", 1)[0]
    sev = severidade(nome)
    enviados.append({"severidade": sev, "destino": ROTAS[sev], "mensagem": mensagem,
                     "runbook": "docs/runbooks/<checagem>.md (exemplo)"})


falhas = raise_alerts(checagens, notify=notify)
print(f"{len(falhas)} de {len(checagens)} checagens falharam\n")
for alerta in enviados:
    print(json.dumps(alerta, ensure_ascii=False, indent=1))

# %% [markdown]
# **Para onde o alerta iria** (☁️ — exige rede e credencial; código de referência):
#
# | Destino | Quando | Como |
# |---|---|---|
# | **E-mail** | informativo, resumo diário, público amplo | notificação nativa do job / do alerta SQL do Databricks |
# | **Teams** | aviso para o time dono, em horário comercial | *notification destination* do Databricks ou webhook de um fluxo do Teams/Power Automate (`requests.post(url, json=...)`), com a URL num secret |
# | **PagerDuty** | quebra de SLA, precisa de alguém agora | *notification destination* PagerDuty ou a Events API (`routing_key` em secret, `dedup_key` para não abrir dez incidentes iguais) |
#
# ```python
# # ☁️ referência — não executado aqui
# import requests
# url = dbutils.secrets.get("ops", "teams-webhook-url")        # nunca a URL no código
# requests.post(url, json={"text": mensagem}, timeout=10).raise_for_status()
# ```
#
# ### 8.2 Schema e distribuição
#
# **Schema:** o contrato é o envelope declarado em `GH_EVENT_SCHEMA` mais as colunas de linhagem que a bronze
# acrescenta. Comparar o schema real com o contrato pega coluna nova, coluna sumida e tipo trocado.
# **Distribuição:** proporções por hora — se o perfil muda de uma hora para outra sem motivo, algo mudou na
# fonte ou no pipeline.

# %%
contrato = {f.name: f.dataType.simpleString() for f in GH_EVENT_SCHEMA.fields}
contrato |= {"_source_file": "string", "_ingested_at": "timestamp", "event_date": "date"}
atual = {f.name: f.dataType.simpleString() for f in bronze_raw.schema.fields}
novas = sorted(atual.keys() - contrato.keys())
sumiram = sorted(contrato.keys() - atual.keys())
tipo_mudou = sorted(c for c in atual.keys() & contrato.keys() if atual[c] != contrato[c])
print(f"schema: {len(atual)} colunas | novas={novas} | sumiram={sumiram} | tipo mudou={tipo_mudou}"
      f" → {'OK' if not (novas or sumiram or tipo_mudou) else 'ALERTA'}")

dist = (
    bronze.where(F.col("event_hour").isin(HORAS))
    .groupBy("event_hour")
    .agg(
        F.count("*").alias("eventos"),
        F.round(100 * F.avg((F.col("type") == "PushEvent").cast("int")), 1).alias("pct_push"),
        F.round(100 * F.avg(F.col("org").isNull().cast("int")), 1).alias("pct_sem_org"),
        F.round(100 * F.avg((F.col("actor.login") == "github-actions[bot]").cast("int")), 1).alias("pct_actions_bot"),
        F.countDistinct("type").alias("tipos"),
    )
    .orderBy("event_hour")
)
dist.show(truncate=False)

linhas_dist = dist.collect()
LIMITE_PP = 10.0  # pontos percentuais
for metrica in ("pct_push", "pct_sem_org", "pct_actions_bot"):
    base = sum(r[metrica] for r in linhas_dist[:-1]) / max(1, len(linhas_dist) - 1)
    desvio = abs(linhas_dist[-1][metrica] - base)
    print(f"{metrica:<16} última hora {linhas_dist[-1][metrica]:5.1f}% | média das anteriores {base:5.1f}%"
          f" | desvio {desvio:4.1f} p.p. → {'ALERTA' if desvio > LIMITE_PP else 'OK'}")

# %% [markdown]
# ### 8.3 Lineage
#
# A bronze carrega `_source_file` em toda linha: é **lineage por linha** — dado um evento estranho, sei de qual
# arquivo ele veio; dado um arquivo ruim, sei quantas linhas reprocessar. O lineage **entre tabelas** (quem lê
# de quem, até o dashboard) é capturado automaticamente pelo Unity Catalog ☁️.

# %%
(
    bronze_raw.groupBy(F.element_at(F.split("_source_file", "/"), -1).alias("arquivo_de_origem"))
    .agg(F.count("*").alias("linhas"), F.min("created_at").alias("primeiro_evento"), F.max("created_at").alias("ultimo_evento"))
    .orderBy("arquivo_de_origem")
    .show(5, truncate=False)
)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Eu monitoro cinco sinais no dado: freshness, volume, schema, distribuição e
# > lineage. Freshness e volume primeiro — são baratos e pegam a maioria dos incidentes. Cada checagem é uma
# > regra com mensagem que diz observado, esperado e tabela; a severidade decide o destino: quebra de SLA vai
# > para o PagerDuty, desvio vai para o canal do time, o resto para o resumo por e-mail. Alerta sem dono e sem
# > runbook eu não crio."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Freshness do evento × da carga:** `max(created_at)` mede o dado; `max(_ingested_at)` mede o pipeline.
#   Fonte parada com pipeline saudável dá carga recente e evento velho — por isso medir os dois.
# - **Mediana no volume:** `check_volume` compara com a mediana do histórico; um dia anômalo no passado não
#   move a régua (há teste para isso). Com histórico curto, como aqui, a régua é fraca: é melhor começar
#   com um piso absoluto ("pelo menos N linhas") e trocar pela mediana quando houver semanas de dados.
# - **Sazonalidade:** volume de domingo ≠ segunda. Linha de base por dia da semana e hora, ou tolerância larga.
# - **Fadiga de alerta** (*alert fatigue*): alerta que dispara toda hora deixa de ser lido. Agrupar,
#   deduplicar, silenciar em manutenção e revisar todo alerta que não levou a uma ação.
# - **Schema na bronze × na silver:** na bronze, coluna nova costuma ser aviso (o dado bruto é preservado); na
#   silver/gold, mudança de schema é quebra de contrato (notebook 08).
# - **Ferramentas:** no Databricks, *data quality monitoring* do Unity Catalog (detecção de anomalia de
#   freshness e completude, e *data profiling*, o antigo Lakehouse Monitoring) ☁️; fora, Monte Carlo, Soda, Great Expectations,
#   elementary (dbt). O mecanismo é este; a ferramenta poupa escrever e manter.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Checagem de distribuição em toda coluna de toda tabela gera custo e falso positivo. Priorize colunas que
#   entram em métrica de negócio ou em chave de join.
# - Limite estático (10 p.p.) é fácil de explicar e precisa de manutenção; detecção estatística se adapta e é
#   mais difícil de justificar quando dispara.

# %% [markdown]
# ## 9. SLI, SLO e SLA de dados 🧪
#
# **O que é**
# - **SLI** (*Service Level Indicator*, indicador): o número medido. Ex.: "% das execuções horárias que
#   terminaram com sucesso", "idade do dado mais novo às 8h".
# - **SLO** (*Service Level Objective*, objetivo): a meta interna para o SLI. Ex.: "99% das execuções com
#   sucesso em 30 dias", "dado com menos de 2 h de idade em 95% das horas".
# - **SLA** (*Service Level Agreement*, acordo): o compromisso com o consumidor, com consequência se quebrar.
#   É mais frouxo que o SLO — a folga entre os dois é a margem para reagir.
# - **Error budget** (*orçamento de erro*): o quanto se pode falhar sem quebrar o SLO (`1 − SLO`).
#
# **Por que importa** — Sem SLO, toda falha é urgente e nenhuma é. Com SLO, a pergunta "paro a entrega de
# funcionalidade para estabilizar?" tem resposta objetiva: o orçamento de erro acabou ou não.
#
# **Como funciona** — o SLI de sucesso sai direto da tabela de execuções. As metas abaixo são **exemplos**.

# %%
sli = spark.sql("""
    WITH por_run AS (
        SELECT run_id, max(CAST(status = 'FAILED' AS INT)) AS falhou FROM pipeline_runs GROUP BY run_id
    )
    SELECT count(*) AS execucoes, sum(falhou) AS falhas FROM por_run
""").first()
sli_sucesso = 1 - sli["falhas"] / sli["execucoes"]

SLO_SUCESSO = 0.99  # exemplo de meta
EXECUCOES_30D = 24 * 30  # pipeline horário
orcamento = (1 - SLO_SUCESSO) * EXECUCOES_30D
print(f"SLI medido aqui  : {sli['execucoes'] - sli['falhas']} de {sli['execucoes']} execuções com sucesso = {sli_sucesso:.0%}")
print(f"SLO (exemplo)    : {SLO_SUCESSO:.0%} em 30 dias → {EXECUCOES_30D} execuções → orçamento de erro = {orcamento:.1f} falhas")
print(f"esta falha gastou: {sli['falhas'] / orcamento:.0%} do orçamento do mês")
print(f"SLO cumprido nesta amostra? {'sim' if sli_sucesso >= SLO_SUCESSO else 'não'} (amostra de {sli['execucoes']} execuções: pequena demais para concluir)")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "SLI é o que eu meço, SLO é a meta interna, SLA é o que prometo ao consumidor, com
# > folga em relação ao SLO. Para dados eu defino por tabela crítica: freshness (dado disponível até tal hora),
# > completude (volume dentro da faixa) e sucesso das execuções. O orçamento de erro decide prioridade: se
# > estourou, o time para de entregar funcionalidade e estabiliza."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **SLO do consumidor, não do job:** "o job terminou às 6h" não interessa; "o dashboard das 8h tem o dado de
#   ontem completo" interessa. Meça na ponta (a tabela gold), não no meio.
# - **Dimensões usuais em dados:** freshness, completude, correção (regras de qualidade), disponibilidade (a
#   tabela está consultável) e tempo de recuperação quando quebra.
# - **Nem toda tabela merece SLO:** classificar por criticidade (tier 1 = decisão/financeiro/regulatório).
# - **Retry conta?** Depende da definição — escreva-a. Se o SLO é de freshness, um retry que entrega no prazo
#   não consome orçamento; se é de sucesso na primeira tentativa, consome.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - SLO de 100% não existe: cada "9" a mais custa caro (redundância, plantão). Negocie a meta com o consumidor
#   a partir do custo da indisponibilidade para ele.
# - SLA sem medição automática do SLI vira discussão de opinião no primeiro incidente.

# %% [markdown]
# ## 10. FinOps: quanto custa e onde se economiza 🧪 (cálculo) / ☁️ (plataforma)
#
# **O que é** — **FinOps** é a prática de dar dono, visibilidade e meta ao custo de nuvem. No Databricks
# clássico a conta de computação tem duas parcelas:
#
# ```text
# custo = DBUs consumidas × preço do DBU          (fatura Databricks)
#       + horas de VM × preço da VM               (fatura do provedor — no Azure, a mesma fatura, linhas separadas)
#
# DBUs = horas × nº de nós × DBU/hora do tipo de VM
# ```
#
# **DBU** (*Databricks Unit*) é a unidade de consumo: cada tipo de VM emite uma quantidade de DBU por hora, e o
# **preço do DBU depende do tipo de compute** (job, all-purpose, SQL, serverless), do plano e da região. Em
# **serverless** não há parcela de VM separada: ela está embutida no DBU.
#
# **Por que importa** — A mesma carga pode custar várias vezes mais só pela escolha do compute. É a pergunta
# de sênior: "como você reduziria a conta?".
#
# **Como funciona** — as alavancas, da mais barata de puxar para a mais cara:
#
# | Alavanca | O que é | Quando compensa | Cuidado |
# |---|---|---|---|
# | **Job compute** em vez de all-purpose | cluster criado para o job e destruído no fim, DBU mais barato | todo job agendado | all-purpose é para trabalho interativo |
# | **Auto-termination** | desliga o cluster interativo depois de N min ocioso | sempre | padrão alto demais = pagar madrugada |
# | **Autoscaling** | nº de workers varia entre mín. e máx. conforme a fila | carga variável | em streaming, o autoscaling clássico reage mal; workers novos demoram minutos |
# | **Azure Spot VMs** | VM com desconto grande, que o Azure pode retomar | workers de job batch tolerante a reexecução | **driver sempre on-demand**; perder worker = recomputar shuffle |
# | **Serverless** | compute gerenciado, início em segundos, paga pelo uso | carga curta/intermitente, times sem quem cuide de cluster | menos controle (rede, versões, tuning); medir o custo real |
# | **SQL warehouse** | compute para SQL/BI, com auto-stop e fila | dashboards e consultas ad hoc | dimensionar pelo pico de concorrência |
# | **Photon** | motor vetorizado em C++; mais rápido, emite mais DBU/hora | joins/agregações grandes em SQL/DataFrame | UDF Python e job curto não aproveitam |
# | **Cluster policies** | regras que limitam o que se pode criar (tipos de VM, máx. de nós, tags, auto-termination) | sempre, em qualquer time | política rígida demais vira fila de exceções |
# | **Tags** | etiquetas no compute, propagadas à fatura | sempre: é o que permite *chargeback* | sem obrigar por policy, ninguém preenche |
#
# ### Exemplo numérico — ILUSTRATIVO
#
# **Os preços abaixo são valores de exemplo, escolhidos só para mostrar a conta. Não são a tabela de preços.
# Conferir a tabela de preços atual** (página de preços do Azure Databricks e das VMs, por região e plano; ou
# `system.billing.list_prices`) antes de usar qualquer número. O que vale levar daqui é a **estrutura** do
# cálculo e a ordem de grandeza relativa entre os cenários.
#
# Cenário: um job de 2 h por dia, 30 dias, num cluster de 1 driver + 4 workers.

# %%
# ===== VALORES ILUSTRATIVOS — conferir a tabela de preços atual =====
DBU_POR_NO_HORA = 0.75       # DBU/hora emitida por nó (depende do tipo de VM)
PRECO_DBU = {"all_purpose": 0.55, "jobs": 0.30}  # US$/DBU por tipo de compute
PRECO_VM_HORA = 0.30         # US$/hora por VM on-demand
DESCONTO_SPOT = 0.70         # desconto da VM spot sobre a on-demand
FATOR_DBU_PHOTON = 2.0       # Photon emite mais DBU/hora na mesma VM
# ====================================================================

NOS, WORKERS = 5, 4
HORAS_MES = 2 * 30

all_purpose = estimate_cost(HORAS_MES, NOS, DBU_POR_NO_HORA, PRECO_DBU["all_purpose"], PRECO_VM_HORA)
job = estimate_cost(HORAS_MES, NOS, DBU_POR_NO_HORA, PRECO_DBU["jobs"], PRECO_VM_HORA)
# Spot só nos workers; driver on-demand. Preço médio de VM por nó:
vm_media_spot = (PRECO_VM_HORA + WORKERS * PRECO_VM_HORA * (1 - DESCONTO_SPOT)) / NOS
job_spot = estimate_cost(HORAS_MES, NOS, DBU_POR_NO_HORA, PRECO_DBU["jobs"], vm_media_spot)

print("ILUSTRATIVO — job de 2 h/dia × 30 dias, 1 driver + 4 workers")
print(f"{'cenário':<34}{'DBUs':>7}{'DBU US$':>10}{'VM US$':>9}{'total US$':>11}{'vs all-purpose':>16}")
for nome, c in [("all-purpose, on-demand", all_purpose), ("job compute, on-demand", job),
                ("job compute, workers em spot", job_spot)]:
    print(f"{nome:<34}{c.dbus:>7.0f}{c.dbu_cost:>10.2f}{c.vm_cost:>9.2f}{c.total:>11.2f}{c.total / all_purpose.total - 1:>15.0%}")

# Photon: mesma VM, mais DBU por hora. Quanto mais rápido precisa ficar para empatar?
custo_hora_normal = estimate_cost(1, NOS, DBU_POR_NO_HORA, PRECO_DBU["jobs"], PRECO_VM_HORA).total
custo_hora_photon = estimate_cost(1, NOS, DBU_POR_NO_HORA * FATOR_DBU_PHOTON, PRECO_DBU["jobs"], PRECO_VM_HORA).total
speedup_empate = custo_hora_photon / custo_hora_normal
print(f"\nPhoton: US$ {custo_hora_normal:.3f}/h sem × US$ {custo_hora_photon:.3f}/h com →"
      f" só compensa em custo se o job ficar pelo menos {speedup_empate:.2f}× mais rápido")

# Ociosidade: cluster all-purpose ligado sem uso até o auto-termination, uma vez por dia útil.
custo_hora_ap = estimate_cost(1, NOS, DBU_POR_NO_HORA, PRECO_DBU["all_purpose"], PRECO_VM_HORA).total
for minutos in (120, 20):
    print(f"ociosidade com auto-termination de {minutos:>3} min × 22 dias úteis: US$ {custo_hora_ap * minutos / 60 * 22:7.2f}/mês")

# Serverless: sem parcela de VM. Até quantos DBUs o serverless empata com o job clássico on-demand?
PRECO_DBU_SERVERLESS = 0.45  # ILUSTRATIVO
print(f"\nserverless a US$ {PRECO_DBU_SERVERLESS}/DBU (ilustrativo) empata com o job clássico (US$ {job.total:.2f}) em"
      f" {job.total / PRECO_DBU_SERVERLESS:.0f} DBUs — o clássico consumiu {job.dbus:.0f}. O consumo real só se mede rodando.")

# %% [markdown]
# **Leitura do exemplo (com os valores ilustrativos acima):**
#
# - Trocar all-purpose por job compute muda **só o preço do DBU**: mesmos 225 DBUs, mesma VM, total 26% menor
#   neste exemplo. É a economia mais barata de obter — basta agendar o job em job compute.
# - Spot mexe **só na parcela de VM** (de US$ 90,00 para US$ 39,60 aqui); o DBU continua igual. Somado ao job
#   compute, o total fica 50% abaixo do all-purpose. Quanto maior a fatia de VM no total, mais o spot rende.
# - Photon não é "mais barato" nem "mais caro" por si: a hora fica mais cara e, com estes valores, o job
#   precisa ficar pelo menos 1,43× mais rápido para empatar. Mede-se rodando a carga real com e sem.
# - Ociosidade é dinheiro sem trabalho nenhum: a diferença entre as duas linhas de auto-termination é só
#   configuração.
# - Serverless não se compara por "nós × horas": compara-se o DBU **medido** em `system.billing.usage` com o
#   custo total do clássico (DBU + VM + o tempo de subida do cluster, que também é cobrado).
#
# > 🎤 **Resposta de 30 s:** "A conta é DBU vezes preço do DBU mais a VM. Eu ataco na ordem: job em job compute
# > e não em all-purpose; auto-termination curto nos interativos; spot nos workers de batch, com driver
# > on-demand; autoscaling com teto; Photon só onde a medição mostra que o ganho de tempo paga o DBU maior;
# > serverless para carga curta e intermitente. E tudo com tag obrigatória por policy, para o custo ter dono —
# > acompanho por `system.billing.usage` cruzado com `list_prices`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **O que `system.billing.usage` não tem:** no Azure, o custo da **VM, disco e rede** do compute clássico
#   aparece no Azure Cost Management, não nas system tables. As tags do cluster são propagadas aos recursos
#   Azure, e é por elas que se juntam as duas metades.
# - **Preço de lista × preço pago:** `list_prices` traz o preço de lista; desconto de contrato e de compromisso
#   pré-pago não aparece ali. Serve para tendência e rateio, não para bater com a fatura ao centavo.
# - **Spot:** `first_on_demand: 1` mantém o driver on-demand; `SPOT_WITH_FALLBACK_AZURE` cai para on-demand se
#   não houver spot. Para streaming com estado ou job com prazo apertado, o risco de retomada costuma não valer.
# - **Autoscaling:** reduz custo em carga variável, mas subir worker leva minutos e descer worker com shuffle
#   em uso obriga a recomputar. Para job curto e previsível, tamanho fixo costuma ser mais barato e mais estável.
# - **Pool de instâncias:** VMs paradas prontas reduzem o tempo de subida; a VM parada no pool paga Azure (sem
#   DBU). Serverless tornou pools menos necessários.
# - **Custo que não é compute:** armazenamento (versões antigas sem `VACUUM`, arquivos pequenos), tráfego de
#   saída entre regiões, e o próprio log (Log Analytics cobra por GB ingerido).
# - **Otimizar a consulta também é FinOps:** o stage com shuffle desnecessário (notebook 09) custa DBU toda
#   noite. Custo por execução na tabela de runs + `job_group` aponta onde.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Otimizar custo de um job que custa pouco é desperdício de engenheiro: ordene por custo mensal e ataque o topo.
# - Cluster compartilhado all-purpose para "economizar" rodando jobs: mais caro por DBU, sem isolamento, e um
#   job pesado derruba os notebooks de todo mundo.
# - Economia que quebra SLA (spot em job com prazo, cluster pequeno demais) sai mais cara que a conta.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# Nada desta seção roda localmente: depende de Unity Catalog, da conta Databricks e do Azure. O código é de
# referência — **confira nomes de colunas na documentação antes de usar**; as system tables ganham colunas com
# frequência e algumas ainda estão em preview.
#
# ### System tables: a observabilidade que a plataforma já grava
#
# **System tables** são tabelas somente leitura no catálogo `system`, mantidas pelo Databricks, com os dados
# operacionais da conta inteira. Um administrador habilita cada schema e concede `SELECT`. Substituem boa parte
# do que se fazia chamando APIs ou lendo logs.
#
# | Tabela | O que responde |
# |---|---|
# | `system.billing.usage` | consumo (DBU e outras unidades) por SKU, workspace, job, cluster, warehouse, tags |
# | `system.billing.list_prices` | preço de lista por SKU ao longo do tempo |
# | `system.lakeflow.job_run_timeline` (e `jobs`, `job_task_run_timeline`) | execuções de jobs e tasks: início, fim, resultado |
# | `system.access.audit` | log de auditoria: quem fez o quê, quando, de onde |
# | `system.access.table_lineage` / `column_lineage` | lineage capturado pelo Unity Catalog |
# | `system.query.history` | consultas em SQL warehouses e serverless: texto, duração, bytes, quem executou |
# | `system.compute.clusters` (e `node_timeline`) | configuração dos clusters ao longo do tempo; uso de CPU/memória por nó |
#
# **Custo por job, em preço de lista, nos últimos 30 dias:**
#
# ```sql
# SELECT u.usage_metadata.job_id                          AS job_id,
#        u.sku_name,
#        round(sum(u.usage_quantity), 1)                  AS dbus,
#        round(sum(u.usage_quantity * lp.pricing.default), 2) AS custo_lista
# FROM system.billing.usage u
# JOIN system.billing.list_prices lp
#   ON  u.sku_name = lp.sku_name AND u.cloud = lp.cloud
#   AND u.usage_start_time >= lp.price_start_time
#   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
# WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
#   AND u.usage_metadata.job_id IS NOT NULL
# GROUP BY ALL
# ORDER BY custo_lista DESC
# LIMIT 20;
# ```
#
# **Chargeback (rateio) por tag — e o consumo sem dono:**
#
# ```sql
# SELECT coalesce(custom_tags['cost_center'], '(sem tag)') AS centro_de_custo,
#        billing_origin_product,
#        round(sum(usage_quantity), 1)                      AS dbus
# FROM system.billing.usage
# WHERE usage_date >= date_trunc('month', current_date())
# GROUP BY ALL ORDER BY dbus DESC;
# ```
#
# **Duração e taxa de falha por job** — o equivalente de plataforma do §3. Uma execução longa aparece em
# **várias linhas** (fatias de tempo); o `result_state` só vem na última, por isso a agregação por `run_id`:
#
# ```sql
# WITH runs AS (
#   SELECT workspace_id, job_id, run_id,
#          min(period_start_time) AS inicio, max(period_end_time) AS fim,
#          max(result_state)      AS resultado
#   FROM system.lakeflow.job_run_timeline
#   WHERE period_start_time >= current_date() - INTERVAL 7 DAYS
#   GROUP BY ALL
# )
# SELECT job_id, count(*) AS execucoes,
#        round(100 * avg(CASE WHEN resultado <> 'SUCCEEDED' THEN 1 ELSE 0 END), 1) AS pct_falha,
#        round(percentile(timestampdiff(SECOND, inicio, fim), 0.5) / 60, 1)        AS mediana_min,
#        round(percentile(timestampdiff(SECOND, inicio, fim), 0.95) / 60, 1)       AS p95_min
# FROM runs WHERE resultado IS NOT NULL
# GROUP BY job_id ORDER BY pct_falha DESC, p95_min DESC;
# ```
#
# **Auditoria: quem mexeu em permissão ou apagou tabela na última semana:**
#
# ```sql
# SELECT event_time, user_identity.email, action_name, request_params
# FROM system.access.audit
# WHERE event_date >= current_date() - INTERVAL 7 DAYS
#   AND service_name = 'unityCatalog'
#   AND action_name IN ('deleteTable', 'updatePermissions')
# ORDER BY event_time DESC;
# ```
#
# **As consultas mais caras do warehouse** (candidatas a otimização):
#
# ```sql
# SELECT executed_by, total_duration_ms / 1000 AS segundos, read_bytes, left(statement_text, 120) AS consulta
# FROM system.query.history
# WHERE start_time >= current_timestamp() - INTERVAL 1 DAY AND execution_status = 'FINISHED'
# ORDER BY total_duration_ms DESC LIMIT 20;
# ```
#
# **Clusters all-purpose sem auto-termination** (a tabela guarda o histórico de configuração; pega-se a linha
# mais recente de cada cluster):
#
# ```sql
# SELECT cluster_id, cluster_name, owned_by, auto_termination_minutes
# FROM system.compute.clusters
# WHERE delete_time IS NULL AND cluster_source IN ('UI', 'API')
# QUALIFY row_number() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
#     AND coalesce(auto_termination_minutes, 0) = 0;
# ```
#
# ### Alertas SQL do Databricks
#
# Um **alerta SQL** é uma consulta agendada + uma condição + destinos de notificação (e-mail, Teams, Slack,
# PagerDuty, webhook). É o lugar natural das checagens do §8 em produção — a regra de freshness vira:
#
# ```sql
# -- alerta: dispara quando horas_de_atraso > 2
# SELECT timestampdiff(MINUTE, max(_ingested_at), current_timestamp()) / 60.0 AS horas_de_atraso
# FROM lake.bronze.gh_events;
# ```
#
# Para falha e duração de job, o próprio job tem notificações (falha, sucesso, **duração acima do limite**) e
# *health rules*. Não é preciso um alerta SQL para "o job falhou".
#
# ### Azure Monitor e Log Analytics
#
# No workspace Azure Databricks (plano Premium), **diagnostic settings** enviam os logs de diagnóstico
# (auditoria por serviço: clusters, jobs, notebooks, Unity Catalog, SQL…) para um **Log Analytics workspace**,
# uma storage account ou um Event Hub. Serve para centralizar com o resto do Azure e alimentar o SIEM do time
# de segurança. A consulta é em KQL:
#
# ```kusto
# DatabricksJobs
# | where TimeGenerated > ago(1d) and ActionName == "runFailed"
# | project TimeGenerated, Identity, RequestParams
# ```
#
# Hoje as **system tables** cobrem a maior parte da análise dentro do Databricks; o Azure Monitor entra quando
# a empresa quer um painel único de operação ou retenção/roteamento fora da plataforma. Métricas de VM (CPU,
# memória) dos nós vêm de `system.compute.node_timeline` ou do Azure Monitor das VMs.
#
# ### Tags, policies e orçamento
#
# ```json
# {
#   "cluster_type":             { "type": "fixed", "value": "job" },
#   "autotermination_minutes":  { "type": "range", "maxValue": 30, "defaultValue": 20 },
#   "autoscale.max_workers":    { "type": "range", "maxValue": 8 },
#   "node_type_id":             { "type": "allowlist", "values": ["Standard_D4ds_v5", "Standard_D8ds_v5"] },
#   "custom_tags.cost_center":  { "type": "allowlist", "values": ["dados", "produto", "financas"] },
#   "azure_attributes.availability": { "type": "fixed", "value": "SPOT_WITH_FALLBACK_AZURE" }
# }
# ```
#
# - Uma **cluster policy** como a acima obriga a tag de centro de custo e limita tamanho e ociosidade: é o
#   controle preventivo. As system tables são o controle detectivo.
# - Para **serverless** não há cluster para etiquetar: a atribuição é feita por *serverless budget policies*
#   (a documentação de 2026 também as chama de *serverless usage policies*): o uso serverless de quem recebe a
#   política sai etiquetado com as tags dela (doc. Azure Databricks, consultada em 05/10/2026).
# - **Budgets** no console da conta avisam quando o gasto de um filtro (workspace, tag) passa do limite — é
#   aviso, não bloqueio.
#
# No bundle deste repositório (`databricks.yml`, notebook 13), o job já declara job cluster, autoscaling e
# tags — o FinOps começa no código revisado, não no painel.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Como você sabe que um pipeline está saudável?**
# <details><summary>Resposta</summary>
# Duas perguntas separadas. Execução: terminou, em quanto tempo, com que taxa de falha (tabela de execuções /
# system tables). Dado: freshness, volume, schema e distribuição dentro do esperado. Job verde com dado errado é
# o caso que só a segunda pega.
# </details>
#
# **2. Quais são os pilares da observabilidade e como se aplicam a dados?**
# <details><summary>Resposta</summary>
# Métricas (duração, linhas, taxa de falha), logs (estruturados, com id de correlação) e traces (execução →
# etapas → stages do Spark). Em dados somam-se lineage e os sinais do próprio dado: freshness, volume, schema,
# distribuição.
# </details>
#
# **3. Como registrar a execução de uma etapa sem esconder a falha?**
# <details><summary>Resposta</summary>
# Context manager com o registro no <code>finally</code>: grava status, duração e erro e relança a exceção. Se a
# gravação do registro falhar, vai para o log e o erro original segue. Aqui: <code>track_step</code>, com teste
# para os dois casos.
# </details>
#
# **4. Como obter linhas gravadas sem rodar um `count()`?**
# <details><summary>Resposta</summary>
# No Delta, <code>operationMetrics</code> do commit (<code>numOutputRows</code>, <code>numTargetRowsInserted</code>…),
# via <code>DESCRIBE HISTORY</code>. Em streaming, <code>numInputRows</code> do progresso. Dentro de uma
# transformação, <code>df.observe</code>.
# </details>
#
# **5. Diferença entre SLI, SLO e SLA? Dê um exemplo em dados.**
# <details><summary>Resposta</summary>
# SLI é a medida (idade do dado às 8h), SLO é a meta interna (menos de 2 h em 95% dos dias), SLA é o compromisso
# com o consumidor, mais frouxo que o SLO. O orçamento de erro (1 − SLO) decide quando parar de entregar para
# estabilizar.
# </details>
#
# **6. Como monitorar um stream?**
# <details><summary>Resposta</summary>
# Pelo progresso de cada micro-lote (<code>StreamingQueryListener</code> ou <code>lastProgress</code>): linhas
# processadas × recebidas por segundo, duração do lote × intervalo do gatilho, backlog da fonte, tamanho do
# estado e watermark. Alerta quando o atraso persiste por vários lotes.
# </details>
#
# **7. O job está verde, mas o dashboard mostra metade dos dados. Como você investiga?**
# <details><summary>Resposta</summary>
# Volume por carga na tabela de execuções e no <code>DESCRIBE HISTORY</code> (onde caiu?); lineage para subir
# até a origem; <code>_source_file</code> para ver se faltou arquivo ou veio truncado; schema e distribuição para
# ver se um filtro/join passou a descartar. Depois, a checagem de volume que teria pegado vira alerta.
# </details>
#
# **8. O que o `DESCRIBE HISTORY` dá e o que ele não dá?**
# <details><summary>Resposta</summary>
# Dá a trilha de escrita: quem, quando, operação, parâmetros e métricas por commit, pelo período de retenção do
# log. Não dá leitura (quem consultou) — isso é <code>system.access.audit</code> — nem alerta sozinho.
# </details>
#
# **9. Como se calcula o custo de um job no Databricks?**
# <details><summary>Resposta</summary>
# DBUs (horas × nós × DBU/hora da VM) × preço do DBU do tipo de compute, mais a VM do provedor; em serverless a
# VM está no DBU. Na prática: <code>system.billing.usage</code> × <code>list_prices</code> por
# <code>usage_metadata.job_id</code>, e a VM no Azure Cost Management, juntando pelas tags.
# </details>
#
# **10. A conta do Databricks dobrou este mês. O que você faz?**
# <details><summary>Resposta</summary>
# <code>system.billing.usage</code> por SKU, workspace, job e tag para achar o que cresceu; comparar com o mês
# anterior. Suspeitos usuais: job em all-purpose, cluster interativo sem auto-termination, job que passou a
# demorar mais (volume ou regressão), warehouse superdimensionado, consumo sem tag. Corrige e cria policy/alerta
# para não voltar.
# </details>
#
# **11. Quando usar Spot e quando não?**
# <details><summary>Resposta</summary>
# Workers de batch idempotente e sem prazo apertado: sim, com driver on-demand e fallback. Driver, streaming com
# estado e job com SLA curto: não — a retomada custa recomputação e pode custar o SLA.
# </details>
#
# **12. Photon sempre compensa?**
# <details><summary>Resposta</summary>
# Não. Ele emite mais DBU por hora; compensa quando o ganho de tempo supera esse fator — tipicamente joins e
# agregações grandes em SQL/DataFrame. UDF Python, job curto ou dominado por I/O ganham pouco. Mede-se com a
# carga real, com e sem.
# </details>

# %% [markdown]
# ## Resumo
#
# - Observe a **execução** (tabela de execuções por etapa, log estruturado com `run_id`, métricas do Spark) e o
#   **dado** (freshness, volume, schema, distribuição, lineage). Job verde não prova dado certo.
# - Registro no `finally`: a falha fica gravada **e** a exceção propaga. Linhas gravadas vêm do
#   `operationMetrics` do Delta, sem `count()` extra.
# - `DESCRIBE HISTORY` é auditoria de escrita pronta; `StreamingQueryListener` é a saúde do stream lote a lote.
# - Alerta = regra + mensagem (observado, esperado, tabela) + severidade que decide o destino + runbook.
#   SLI mede, SLO é a meta, SLA é o compromisso; o orçamento de erro decide prioridade.
# - Custo = DBU × preço do DBU + VM. Ordem das alavancas: job compute, auto-termination, spot nos workers,
#   autoscaling com teto, Photon medido, serverless para carga intermitente — e tag obrigatória por policy.
#   No Databricks, tudo isso se consulta em `system.billing`, `system.lakeflow`, `system.access`,
#   `system.query` e `system.compute`.

# %%
spark.stop()
