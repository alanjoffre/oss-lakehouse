# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 08 · Qualidade de dados e contratos: expectations, quarentena, freshness, volume e pipelines declarativos
#
# > Prova que o pipeline **mede** a qualidade regra a regra, **separa** o que é ruim (quarentena com motivo)
# > em vez de apagar em silêncio, **falha** quando o contrato com o consumidor quebra — e mostra o que o
# > Spark Declarative Pipelines open source faz (e não faz) em relação ao Lakeflow.
#
# | Requisito da vaga | Onde aparece aqui |
# |---|---|
# | Arquitetura de pipelines | expectations warn/drop/fail, quarentena, contrato como portão de publicação |
# | Databricks | Lakeflow Declarative Pipelines (`@dp.expect_or_drop`) ☁️, DQX ☁️, constraints Delta 🧪 |
# | Python avançado | `oss_lakehouse.quality` (dataclasses, pydantic, YAML) com testes (`tests/test_quality.py`) |
# | Times multidisciplinares | contrato YAML versionado = acordo entre produtor e consumidor |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup

# %%
import importlib.util
import os
import shutil
import subprocess
import textwrap
from datetime import UTC, datetime
from pathlib import Path

import pyspark.pipelines as sdp
from pyspark.sql import functions as F

from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.quality import (
    ContractViolation,
    Expectation,
    ExpectationFailed,
    apply_expectations,
    check_freshness,
    enforce_contract,
    expectations_from_dicts,
    load_contract,
    validate_data,
    validate_schema,
    volume_anomalies,
)
from oss_lakehouse.spark import get_spark

spark = get_spark("08")
s = get_settings()
silver = spark.read.format("delta").load(s.path("silver", "gh_events")).cache()
CONTRATO = PROJECT_ROOT / "contracts" / "silver_gh_events.yaml"
QUARENTENA = s.path("quarantine", "silver_gh_events")
DEMO = Path(s.data_root) / "demo" / "08"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True)
print(f"silver.gh_events: {silver.count():,} linhas")

# %% [markdown]
# ## 1. Expectations declarativas: warn, drop, fail
#
# **O que é** — uma *expectation* (expectativa) é uma regra de qualidade declarada como **dado**: nome +
# condição SQL que a linha válida satisfaz + ação quando falha:
#
# | Ação | O que acontece | Quando usar |
# |---|---|---|
# | `warn` | a linha segue; a falha só é **medida** | regra nova (observar antes de bloquear), anomalia tolerável |
# | `drop` | a linha sai do fluxo e vai para a **quarentena** com o motivo | registro individual inválido |
# | `fail` | o lote inteiro **aborta** | violação que torna tudo suspeito (chave nula, schema errado) |
#
# **Por que importa** — sem isso, dado ruim ou passa calado para o dashboard, ou é filtrado por um `WHERE`
# escondido que ninguém mede. Quarentena com motivo permite reprocessar depois de corrigir.
#
# **Como funciona** — `apply_expectations` avalia todas as regras numa passada, devolve válidos, quarentena
# (com `_dq_failed_rules`) e métricas por regra. Condição que dá NULL conta como **falha**. As regras vêm do
# contrato YAML (seção 2) + duas regras extras de negócio:

# %%
contrato = load_contract(CONTRATO)
regras = expectations_from_dicts(contrato.expectations) + [
    Expectation("created_at_nao_futuro", "created_at <= current_timestamp()", "drop"),
    Expectation("evento_na_hora_do_arquivo",
                "event_hour = CAST(regexp_extract(_source_file, '-([0-9]+)[.]json[.]gz$', 1) AS INT)", "warn"),
]
for r in regras:
    print(f"{r.action:<5} {r.name:<26} {r.condition[:70]}")

# %%
res = apply_expectations(silver, regras)
res.metrics_df(spark).select("rule", "action", "failed", "pass_rate").show(truncate=False)

# %% [markdown]
# A Silver real passa nas regras de bloqueio (é o esperado: ela já foi limpa no notebook 05). Para ver
# drop/quarentena e fail, um **lote sujo simulado** (explícito: 5 linhas reais adulteradas):

# %%
amostra = silver.orderBy("event_id").limit(5)
sujo = (amostra
        .withColumn("_i", F.monotonically_increasing_id())
        .withColumn("repo_id", F.when(F.col("_i") == 0, None).otherwise(F.col("repo_id")))
        .withColumn("created_at", F.when(F.col("_i") == 1, F.lit("2030-01-01").cast("timestamp"))
                    .otherwise(F.col("created_at")))
        .withColumn("event_type", F.when(F.col("_i") == 2, F.lit("EventoInventado")).otherwise(F.col("event_type")))
        .drop("_i"))
lote = silver.unionByName(sujo.withColumn("event_id", -F.col("event_id")))  # ids negativos: não colidem com os reais

res = apply_expectations(lote, regras)
res.metrics_df(spark).filter("failed > 0").select("rule", "action", "failed").show(truncate=False)
print(f"entrada={lote.count():,}  válidos={res.valid.count():,}  quarentena={res.quarantine.count():,}")
res.quarantine.select("event_id", "event_type", "repo_id", "created_at", "_dq_failed_rules").show(truncate=False)

# %%
res.quarantine.write.format("delta").mode("overwrite").save(QUARENTENA)
print("quarentena gravada em", QUARENTENA.replace(str(PROJECT_ROOT) + "/", ""), "→",
      spark.read.format("delta").load(QUARENTENA).count(), "linhas")

# Histórico de métricas (append): é o que alimenta um painel de qualidade ao longo do tempo.
(res.metrics_df(spark).withColumn("table", F.lit("silver.gh_events"))
 .withColumn("checked_at", F.current_timestamp())
 .write.format("delta").mode("append").save(str(DEMO / "dq_metrics")))

# %% [markdown]
# E a ação `fail`: um evento sem `event_id` aborta o lote inteiro — nada é devolvido para gravar.

# %%
sem_id = lote.unionByName(amostra.limit(1).withColumn("event_id", F.lit(None).cast("bigint")))
try:
    apply_expectations(sem_id, regras)
except ExpectationFailed as e:
    print("ABORTADO:", e)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Regra de qualidade para mim é configuração: nome, condição SQL e ação — warn
# > mede, drop manda para quarentena com o motivo, fail aborta o lote. Avalio tudo numa passada só e gravo
# > métrica por regra a cada execução, então dá para alertar em tendência. Regra nova entra como warn,
# > observo, e só depois promovo para drop ou fail."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **NULL**: `repo_id > 0` com `repo_id` nulo dá NULL, não FALSE. Aqui NULL = falha (explícito); num
#   `CHECK` constraint SQL, NULL **passa**. Por isso "não nulo" é sempre regra própria.
# - **Custo**: as métricas exigem uma ação (agregação) e depois os DataFrames válido/quarentena são
#   recalculados — em produção, `cache`/`persist` do lote ou gravar com `foreachBatch` uma vez só.
# - **Quarentena** precisa de dono, retenção e caminho de volta (corrigir e reprocessar). Senão vira lixão.
# - **Regras como dado** podem morar numa tabela Delta: mudar regra não exige deploy (mas exige revisão).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `drop` em dado que deveria parar o pipeline esconde incidente — falha sistêmica (50% nulo) merece `fail`
#   ou alerta por **taxa**, não por linha.
# - Muitas regras caras (regex, subconsulta) em tabela grande pesam; regras de linha são baratas, regras de
#   conjunto (unicidade, referencial) custam um shuffle.

# %% [markdown]
# ### A última linha de defesa: constraints do Delta (🧪)
#
# Além das expectations no pipeline, a **tabela** pode recusar escrita inválida: `NOT NULL` e `CHECK`
# são validados pelo Delta em todo commit — de qualquer writer, não só do nosso job.

# %%
tabela_ck = str(DEMO / "com_constraint")
silver.limit(100).write.format("delta").save(tabela_ck)
# Invariante: o GitHub existe desde 2008 e esta tabela só recebe o ano corrente (2026).
spark.sql(f"ALTER TABLE delta.`{tabela_ck}` ADD CONSTRAINT created_at_plausivel "
          "CHECK (created_at >= '2008-01-01' AND created_at < '2027-01-01')")
ruim = sujo.filter(F.year("created_at") == 2030)  # a linha com created_at adulterado para 2030
print("tentando gravar:", ruim.count(), "linha ruim + 4 boas")
try:
    ruim.unionByName(amostra.limit(4)).write.format("delta").mode("append").save(tabela_ck)
    print("gravou (a constraint NÃO barrou)")
except Exception as e:  # noqa: BLE001
    motivo = [ln.strip() for ln in str(e).splitlines() if "constraint" in ln.lower()]
    print("escrita recusada →", (motivo[0] if motivo else type(e).__name__)[:200])
print("linhas na tabela depois da tentativa:", spark.read.format("delta").load(tabela_ck).count(), "(eram 100)")

# %% [markdown]
# A escrita inteira foi recusada — inclusive as 4 linhas boas do mesmo lote (atomicidade do commit): é um
# `fail` aplicado pela tabela. Bom para invariantes que **nunca** podem ser violadas; ruim como único
# mecanismo, porque não há quarentena nem métrica. E a constraint só protege o que ela diz: uma regra frouxa
# (ex.: "ano < 2100") deixaria passar a mesma linha de 2030 que a expectation `created_at_nao_futuro` pegou.

# %% [markdown]
# ## 2. Contrato de dados em YAML, validado contra o schema real
#
# **O que é** — o *data contract* (contrato de dados) é o acordo versionado entre quem produz e quem consome
# a tabela: colunas, tipos, nulos, chave, grão, freshness, dono. Vive no Git (`contracts/silver_gh_events.yaml`),
# muda por PR e segue versionamento semântico (*semver*: mudança que quebra = versão MAJOR).
#
# **Por que importa** — o incidente clássico: o produtor renomeia uma coluna ou muda `bigint` para `string`,
# o job dele passa, e o dashboard do consumidor quebra três dias depois. Com contrato validado **antes de
# publicar**, quem quebra é o job do produtor — na hora, com a mensagem certa.
#
# **Como funciona**

# %%
linhas = CONTRATO.read_text(encoding="utf-8").splitlines()
i_cols = linhas.index("columns:")
print("\n".join(linhas[4:14] + ["…"] + linhas[i_cols:i_cols + 4]))
print("…", len(contrato.columns), "colunas declaradas")

# %%
print("schema real × contrato:", validate_schema(silver.schema, contrato) or "OK")
print("conteúdo (chave única, NOT NULL):", validate_data(silver, contrato) or "OK")
enforce_contract(silver, contrato)  # não levanta nada: pode publicar
print("contrato", contrato.name, contrato.version, "cumprido")

# %% [markdown]
# **Contrato quebrado** — o produtor "só" mudou o tipo do `event_id` para string, removeu `repo_owner`
# e adicionou uma coluna:

# %%
quebrado = (silver.withColumn("event_id", F.col("event_id").cast("string")).drop("repo_owner")
            .withColumn("repo_stars", F.lit(0)))
try:
    enforce_contract(quebrado, contrato)
except ContractViolation as e:
    for p in e.problems:
        print("✗", p)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Contrato de dados é um YAML no Git com colunas, tipos, nulos, chave, grão,
# > freshness e dono. O pipeline valida o schema real contra ele antes de publicar — schema é grátis de
# > checar — e depois chave única e não nulos. Se quebrar, o job do produtor falha, não o dashboard do
# > consumidor. Mudança que quebra vira versão major, com aviso e período de convivência."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Padrão de mercado**: ODCS (*Open Data Contract Standard*, projeto Bitol na Linux Foundation) define um
#   YAML com schema, qualidade, SLA e responsáveis; Soda e DQX leem contratos; dbt tem *model contracts*
#   (`contract: {enforced: true}`) que validam nomes e tipos no build.
# - **Coluna extra quebra?** Depende da política (`allow_extra_columns`). Para consumidor que faz `SELECT *`,
#   sim. Aqui é estrito de propósito.
# - **Schema enforcement do Delta** já recusa escrita com tipo incompatível — o contrato vai além: semântica
#   (grão, chave, freshness) e o lado do **consumidor**.
# - **Evolução sem quebra**: adicionar coluna opcional (minor), período com coluna velha e nova, *views* de
#   compatibilidade.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Contrato sem dono e sem processo de mudança vira documentação desatualizada.
# - Em tabela exploratória/interna de um time só, o custo de manter contrato não compensa.

# %% [markdown]
# ## 3. Freshness: o dado está atualizado?
#
# **O que é** — *freshness* (atualidade): o dado mais novo tem no máximo X de atraso. O contrato diz 3 h.
#
# **Por que importa** — a falha mais silenciosa: o job "deu verde" mas a fonte parou de mandar arquivo.
# Contagem e schema continuam perfeitos; só a idade denuncia.
#
# **Como funciona** — `check_freshness` compara o `max(created_at)` com um "agora". Com o relógio real, este
# dado (de 2026-10-01) está velho; com o relógio da época do processamento, está em dia:

# %%
agora_real = datetime.now(UTC).replace(tzinfo=None)
for rotulo, agora in [("relógio real", agora_real), ("processamento às 16h de 01/10", datetime(2026, 10, 1, 16))]:
    f = check_freshness(silver, contrato.freshness.column, contrato.freshness.max_delay_hours, now=agora)
    print(f"{rotulo:<30} mais novo={f.latest}  atraso={f.lag_hours:>7} h  limite={f.max_delay_hours} h  "
          f"{'OK' if f.ok else 'ATRASADO'}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Freshness é a checagem que pega fonte parada: compara o timestamp mais novo com
# > o agora e o SLA do contrato. Rodo separado do job de carga — se o job nem rodou, quem alerta é o monitor
# > de freshness, não o job."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Freshness pelo **tempo do evento** (`created_at`) × pelo **tempo de ingestão** (`_ingested_at`): o primeiro
#   mede a fonte; o segundo, o pipeline. Os dois são úteis.
# - Fuso horário: tudo em UTC (a sessão Spark está em UTC); comparar com relógio local é bug clássico.
# </details>
#
# **Trade-offs** — fonte naturalmente intermitente (sem eventos à noite) gera falso alarme: o limite precisa
# considerar o calendário da fonte.

# %% [markdown]
# ## 4. Volume: anomalia por z-score nas 24 horas do dia
#
# **O que é** — checar se a quantidade de linhas de um lote é plausível frente ao histórico. *z-score* =
# quantos desvios-padrão o valor está da média.
#
# **Por que importa** — arquivo truncado, fonte que mandou metade, filtro errado num deploy: o schema está
# perfeito, a contagem não.
#
# **Como funciona** — contamos as linhas de cada um dos 24 arquivos horários do dia (landing + `raw_cache`).
# `volume_anomalies` compara cada hora com as **outras** (*leave-one-out*: a anomalia não infla a própria média):

# %%
arquivos = (f"{s.data_root}/landing/gharchive/*.json.gz", f"{s.data_root}/raw_cache/gharchive/*.json.gz")
por_hora = (spark.read.text(list(arquivos)).select(F.col("_metadata.file_name").alias("arq"))
            .groupBy("arq").count().collect())
contagens = {int(r.arq.rsplit("-", 1)[1].split(".")[0]): r["count"] for r in por_hora}
contagens = {f"{h:02d}h": contagens[h] for h in sorted(contagens)}
res_vol = volume_anomalies(contagens, z_threshold=3)
vals = list(contagens.values())
print(f"{len(vals)} horas · mín={min(vals):,} · máx={max(vals):,}")
print("anomalias com z > 3:", [r["key"] for r in res_vol if r["anomaly"]] or "nenhuma")
print("maiores |z|:", sorted(((r["key"], r["z"]) for r in res_vol), key=lambda x: -abs(x[1]))[:3])

# %% [markdown]
# O dado **real** já acusa uma hora: a 00h, a de menor volume do dia (o mínimo impresso acima). É incidente
# ou é o vale normal da madrugada UTC? Com um dia só de histórico **não dá para saber** — é exatamente o
# limite do z-score sem sazonalidade: a referência justa seria a mesma hora de dias anteriores. Em produção
# isso entraria como `warn`, não como bloqueio.
#
# Agora o incidente simulado (explícito): a hora 15 chega **truncada**, com 30% do volume normal.

# %%
incidente = dict(contagens, **{"15h": int(contagens["15h"] * 0.3)})
for r in volume_anomalies(incidente, z_threshold=3):
    if r["anomaly"]:
        print(f"ANOMALIA {r['key']}: {r['value']:,} linhas (média das outras {r['mean']:,.0f} ± {r['stdev']:,.0f}) "
              f"z={r['z']}")

# %% [markdown]
# A hora 15 truncada é pega com folga. Repare no efeito colateral: a 00h **deixou de ser acusada** — o
# incidente da hora 15 inflou o desvio-padrão que serve de referência para as outras horas (*masking*: uma
# anomalia grande esconde uma menor). Mediana + MAD sofrem menos com isso.
#
# > 🎤 **Resposta de 30 s:** "Volume eu checo com estatística simples sobre o histórico: z-score contra as
# > outras janelas, ou limites de ±X% da mesma hora da semana passada. Pega arquivo truncado e fonte que
# > mandou metade — coisas que nenhuma regra de linha pega."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Sazonalidade**: o GitHub tem ciclo diário/semanal. Comparar com a **mesma hora** de semanas anteriores é
#   mais justo que com as horas vizinhas.
# - **Robustez**: z-score usa média e desvio, que a própria anomalia distorce — daí o *leave-one-out*; outra
#   saída é mediana e MAD (desvio absoluto mediano).
# - **Data observability**: Databricks Lakehouse Monitoring / *data quality monitoring* ☁️, Monte Carlo,
#   Soda, Elementary (dbt) fazem isso como produto (freshness + volume + schema + distribuição).
# </details>
#
# **Trade-offs** — com 24 pontos o desvio é instável; limiar baixo = alarme falso, alto = incidente perdido.
# Comece com alerta (warn) e calibre.

# %% [markdown]
# ## 5. Spark Declarative Pipelines (open source) × Lakeflow Declarative Pipelines
#
# **O que é** — pipeline **declarativo**: você declara as tabelas (o *quê*) e o motor resolve o grafo de
# dependências, a ordem, o incremental e os retries (o *como*). O Databricks doou o núcleo do
# Delta Live Tables ao Apache Spark: no PySpark 4.1+ existe `pyspark.pipelines` e a CLI `spark-pipelines`.
# O produto comercial chama-se **Lakeflow Declarative Pipelines** (antigo DLT).
#
# **Por que importa** — expectations, linhagem e incremental deixam de ser código seu e viram recurso do motor.
#
# **Como funciona — o que testei aqui:**
#
# 1. A API OSS **não tem expectations**: não há `expect`/`expect_or_drop` em `pyspark.pipelines`, e o SQL
#    `CONSTRAINT … EXPECT … ON VIOLATION DROP ROW` nem passa pelo parser do Spark 4.2 (erro na célula abaixo).
# 2. A CLI roda sobre **Spark Connect**, que exige pacotes que o projeto não instala (pandas, pyarrow, grpcio):

# %%
print("funções de pyspark.pipelines:", sorted(n for n in dir(sdp) if not n.startswith("_") and n.islower()))
print("tem algo com 'expect'?", [n for n in dir(sdp) if "expect" in n.lower()] or "não")
print("dependências do Spark Connect no .venv:",
      {p: importlib.util.find_spec(p) is not None for p in ("pandas", "pyarrow", "grpc")})
try:
    spark.sql("CREATE MATERIALIZED VIEW mv_teste (CONSTRAINT id_ok EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW) "
              "AS SELECT 1 AS id")
except Exception as e:  # noqa: BLE001
    cond = getattr(e, "getCondition", lambda: None)()
    print("SQL com EXPECT →", type(e).__name__, cond)

# %% [markdown]
# Então o equivalente local é: tabela válida e tabela de quarentena como **duas materialized views** com o
# filtro da regra — expectation "na mão", mas com o grafo, a ordem e a materialização feitos pelo motor.
# O projeto do pipeline:

# %%
sdp_dir = DEMO / "sdp_pipeline"
(sdp_dir / "transformations").mkdir(parents=True)
entrada = DEMO / "sdp_entrada"
entrada.mkdir()
shutil.copy(PROJECT_ROOT / "tests" / "fixtures" / "gharchive" / "2026-10-01-12-sample.json.gz", entrada)
(entrada / "lote_sujo.json").write_text(
    '{"id":"1","type":"PushEvent","repo":{"id":null,"name":"x/y"},"created_at":"2026-10-01T12:00:00Z"}\n'
    '{"id":"abc","type":"PushEvent","repo":{"id":7,"name":"x/z"},"created_at":"2026-10-01T12:00:01Z"}\n',
    encoding="utf-8")

(sdp_dir / "spark-pipeline.yml").write_text(textwrap.dedent(f"""\
    name: gh_events_quality
    storage: file://{sdp_dir}/pipeline-storage
    libraries:
      - glob:
          include: transformations/**
    """), encoding="utf-8")

(sdp_dir / "transformations" / "eventos.py").write_text(textwrap.dedent(f'''\
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    SCHEMA = "id string, type string, repo struct<id:bigint,name:string>, created_at string"
    REGRAS = {{  # expectation "na mão": nome -> condição que a linha válida satisfaz
        "repo_id_not_null": "repo.id IS NOT NULL",
        "id_numerico": "id RLIKE '^[0-9]+$'",
    }}
    VALIDA = " AND ".join(f"coalesce(({{c}}), false)" for c in REGRAS.values())


    @dp.materialized_view(comment="bronze: eventos crus da landing")
    def bronze_events():
        return spark.read.schema(SCHEMA).json("{entrada}")


    @dp.materialized_view(comment="silver: só linhas que passam em todas as regras")
    def silver_events():
        return spark.read.table("bronze_events").where(VALIDA)


    @dp.materialized_view(comment="quarentena: linhas reprovadas + motivo")
    def silver_events_quarentena():
        motivos = F.filter(
            F.array(*[F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in REGRAS.items()]),
            lambda x: x.isNotNull(),
        )
        return spark.read.table("bronze_events").where(f"NOT ({{VALIDA}})").withColumn("_dq_failed_rules", motivos)
    '''), encoding="utf-8")
print((sdp_dir / "transformations" / "eventos.py").read_text(encoding="utf-8")[:900])

# %% [markdown]
# Para rodar, liberamos a sessão Spark deste notebook (a CLI sobe a sua própria JVM com o servidor Spark
# Connect) e chamamos `spark-pipelines run` com as dependências do Connect adicionadas **só para este
# comando** (`uv run --with …`, sem mexer no `pyproject.toml`). Se o cache do `uv` não tiver os pacotes e não
# houver rede, a célula mostra o erro em vez de fingir que rodou.

# %%
spark.stop()
deps = ["pandas>=2.2,<3", "pyarrow>=18", "grpcio>=1.67", "grpcio-status>=1.67",
        "googleapis-common-protos>=1.65", "zstandard"]
cmd = ["uv", "run", "--offline", "--project", str(PROJECT_ROOT)]
for d in deps:
    cmd += ["--with", d]
cmd += ["spark-pipelines", "run"]
env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
t0 = datetime.now()
proc = subprocess.run(cmd, cwd=sdp_dir, env=env, capture_output=True, text=True, timeout=900)
linhas = [ln for ln in (proc.stdout + proc.stderr).splitlines() if ": Flow " in ln or "Run is" in ln or "Error" in ln]
print(f"código de saída: {proc.returncode} · {(datetime.now() - t0).seconds}s")
print("\n".join(ln[:140] for ln in linhas if "COMPLETED" in ln or "FAILED" in ln or "Error" in ln))

# %%
spark = get_spark("08")
wh = sdp_dir / "spark-warehouse"
if proc.returncode == 0:
    for t in ("bronze_events", "silver_events", "silver_events_quarentena"):
        df = spark.read.parquet(str(wh / t))
        print(f"{t:<26} {df.count():>5} linhas")
    spark.read.parquet(str(wh / "silver_events_quarentena")).select("id", "repo", "_dq_failed_rules").show(
        truncate=False)

# %% [markdown]
# O motor descobriu sozinho a ordem (bronze antes das duas silvers, que rodam em paralelo) e materializou as
# três tabelas. O que **faltou** em relação ao Lakeflow: expectations nativas com métricas no *event log*,
# Auto CDC SCD2, tabelas Delta por padrão e Unity Catalog. No Databricks, o mesmo pipeline fica assim ☁️:
#
# ```python
# from pyspark import pipelines as dp     # alias legado: import dlt
#
# @dp.table(comment="silver com expectations")
# @dp.expect_or_fail("event_id_not_null", "event_id IS NOT NULL")
# @dp.expect_all_or_drop({"repo_id_not_null": "repo_id IS NOT NULL",
#                         "created_at_nao_futuro": "created_at <= current_timestamp()"})
# @dp.expect("evento_na_hora_do_arquivo", "event_hour = hour_from_file")   # warn: só métrica
# def silver_gh_events():
#     return spark.readStream.table("bronze_gh_events").select(...)
# ```
#
# ```sql
# CREATE OR REFRESH STREAMING TABLE silver_gh_events (
#   CONSTRAINT event_id_not_null EXPECT (event_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
#   CONSTRAINT repo_id_not_null  EXPECT (repo_id IS NOT NULL)  ON VIOLATION DROP ROW,
#   CONSTRAINT tipo_conhecido    EXPECT (event_type LIKE '%Event')            -- warn
# ) AS SELECT … FROM STREAM(bronze_gh_events);
# ```
#
# As métricas de cada expectation ficam no *event log* do pipeline (`event_type = 'flow_progress'`,
# `details:flow_progress.data_quality.expectations`). Detalhe: `expect_or_drop` **descarta** — não guarda a
# linha. Para quarentena, o padrão é uma segunda tabela com a condição invertida (exatamente o que fizemos
# acima) ou o DQX.
#
# > 🎤 **Resposta de 30 s:** "Pipeline declarativo: declaro tabelas e expectations, o motor faz grafo,
# > ordem, incremental e métricas. O Spark open source herdou o núcleo — rodei aqui com `spark-pipelines` —
# > mas sem expectations: o equivalente é uma view válida e uma de quarentena. No Lakeflow uso `expect`,
# > `expect_or_drop` e `expect_or_fail`, e leio a qualidade no event log."
#
# **Trade-offs / quando NÃO usar**
# - Declarativo tira controle fino (ordem de escrita, MERGE customizado, side effects). Lógica muito específica
#   fica melhor em job comum com `foreachBatch`.
# - No OSS (Spark 4.1/4.2) o recurso é jovem: sem expectations, CLI dependente de Spark Connect.

# %% [markdown]
# ## 6. Ferramentas de mercado: quando usar cada uma
#
# | Ferramenta | Como declara | Onde brilha | Quando não |
# |---|---|---|---|
# | **Lakeflow expectations** ☁️ | decorator / `CONSTRAINT … EXPECT` no pipeline | regra junto da tabela, métrica no event log, zero infra | fora do Lakeflow; quarentena exige tabela extra |
# | **DQX** (Databricks Labs) ☁️/🧪 | checks em YAML ou código PySpark | separa válido × quarentena com motivo (como o `quality.py` daqui), *profiling* sugere regras, roda em batch e streaming | ambiente sem Spark |
# | **Great Expectations** (GX Core) | *expectation suites* em Python/JSON | catálogo enorme de expectativas, *Data Docs* (relatório HTML), multi-engine | pesado de configurar; overhead para regras simples |
# | **Soda** (Soda Core / SodaCL) | YAML (`checks for tabela: - missing_count(x) = 0`) | time de analytics/SQL, checagens por *scan* agendado, contratos | regra linha a linha com quarentena |
# | **dbt tests** | `unique`, `not_null`, `accepted_values`, `relationships` + SQL; *model contracts* | quem já transforma com dbt: teste roda no build, falha o deploy | pipeline Spark/streaming fora do dbt |
# | **Delta constraints** 🧪 | `NOT NULL`, `CHECK` na tabela | invariante absoluta, vale para qualquer writer | sem quarentena nem métrica: recusa o commit inteiro |
#
# Regra prática: **no pipeline** (Lakeflow/DQX/código próprio) para decidir linha a linha; **na tabela**
# (constraints) para invariantes; **por cima** (Soda/GE/dbt tests/monitoramento) para reconciliação, volume,
# freshness e distribuição.
#
# > 🎤 **Resposta de 30 s:** "Dentro do Databricks eu começo com expectations do Lakeflow e uso DQX quando
# > preciso de quarentena com motivo. Se o time é dbt, dbt tests e model contracts. Soda ou Great
# > Expectations para checagens agendadas e relatórios fora do pipeline. E constraints Delta para o que nunca
# > pode entrar."

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# - **Lakeflow Declarative Pipelines**: expectations nativas (`expect`, `expect_or_drop`, `expect_or_fail`,
#   `expect_all*`), métricas no event log, Auto CDC, modo *triggered* ou *continuous*.
# - **DQX**: `pip install databricks-labs-dqx`; `DQEngine(WorkspaceClient()).apply_checks_by_metadata_and_split(df, checks)`
#   devolve (válidos, quarentena) — mesma ideia do `apply_expectations` daqui.
# - **Data quality monitoring / Lakehouse Monitoring** (Unity Catalog): perfil e drift de tabela, freshness e
#   volume automáticos, alertas no Databricks SQL.
# - **Unity Catalog**: dono da tabela, *tags*, comentários de coluna e linhagem — boa parte do "contrato" passa
#   a ter onde morar; constraints `PRIMARY KEY`/`FOREIGN KEY` informativas.
# - **Azure**: alertas de qualidade/freshness via SQL Alerts → e-mail/Teams; Azure Monitor para os jobs.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Qual a diferença entre warn, drop e fail? Quando usa cada uma?**
# <details><summary>Resposta</summary>
# warn mede e deixa passar (regra nova, anomalia tolerável); drop remove a linha (para quarentena, com motivo)
# quando o problema é do registro; fail aborta o lote quando a violação torna tudo suspeito (chave nula, schema).
# </details>
#
# **2. Por que quarentena e não simplesmente filtrar?**
# <details><summary>Resposta</summary>
# Filtro apaga em silêncio. Quarentena preserva a linha e o motivo, permite medir, alertar, corrigir na fonte e
# reprocessar. Precisa de dono e retenção.
# </details>
#
# **3. O que é um data contract e como você o aplica?**
# <details><summary>Resposta</summary>
# Acordo versionado produtor × consumidor (colunas, tipos, nulos, chave, grão, freshness, dono). Valido o schema
# real contra o YAML antes de publicar; quebra = job do produtor falha. Mudança incompatível = versão major.
# </details>
#
# **4. Como você detecta que uma fonte parou de mandar dados?**
# <details><summary>Resposta</summary>
# Freshness: max(timestamp do evento) vs agora contra o SLA, num monitor independente do job de carga (se o
# job nem roda, o monitor ainda alerta).
# </details>
#
# **5. E um arquivo que chegou pela metade?**
# <details><summary>Resposta</summary>
# Checagem de volume contra o histórico (z-score leave-one-out, ou mesma hora da semana anterior ±X%).
# Demonstrado: hora com 30% do volume vira anomalia.
# </details>
#
# **6. Como uma condição com NULL se comporta numa expectation?**
# <details><summary>Resposta</summary>
# `NULL > 0` é NULL. Aqui NULL conta como falha (explícito); em CHECK constraint SQL, NULL passa. Por isso regra
# de não nulo separada.
# </details>
#
# **7. Expectations do Lakeflow guardam as linhas descartadas?**
# <details><summary>Resposta</summary>
# Não — `expect_or_drop` descarta e só registra a contagem no event log. Para quarentena: segunda tabela com a
# condição invertida, ou DQX.
# </details>
#
# **8. O Spark open source tem pipelines declarativos?**
# <details><summary>Resposta</summary>
# Sim, desde o 4.1: `pyspark.pipelines` + CLI `spark-pipelines` (sobre Spark Connect). Rodei aqui. Mas sem
# expectations — é o núcleo do DLT, não o produto inteiro.
# </details>
#
# **9. Great Expectations, Soda, DQX ou dbt tests?**
# <details><summary>Resposta</summary>
# Depende de onde está a transformação: Lakeflow/DQX dentro do Databricks; dbt tests se o time usa dbt; Soda/GE
# para checagens agendadas e relatório fora do pipeline. Constraints Delta para invariantes.
# </details>
#
# **10. Como você evita que regras de qualidade virem alarme que todo mundo ignora?**
# <details><summary>Resposta</summary>
# Toda regra tem dono e ação; regra nova entra como warn e é calibrada; alerta por taxa/tendência, não por
# linha; métricas históricas para ver regressão; revisar regras que nunca falham ou sempre falham.
# </details>

# %% [markdown]
# ## Resumo
#
# - Expectations como dado (nome, condição SQL, ação warn/drop/fail), avaliadas numa passada, com métrica por regra e quarentena com motivo.
# - Contrato YAML versionado validado contra o schema real antes de publicar; quebra → falha do produtor, com a lista do que quebrou.
# - Freshness e volume pegam o que regra de linha não pega: fonte parada, arquivo truncado.
# - Spark Declarative Pipelines OSS roda local (Spark Connect) mas sem expectations; no Lakeflow elas são nativas — quarentena ainda é tabela extra ou DQX.
# - Constraints Delta para invariantes absolutas; Soda/GE/dbt tests/monitoramento por cima.

# %%
spark.stop()
