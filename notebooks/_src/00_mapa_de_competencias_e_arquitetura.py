# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 00 · Mapa de competências e arquitetura
#
# > Este notebook liga cada competência de engenharia de dados a um notebook que o demonstra, desenha a arquitetura alvo na Azure
# > e a equivalente local, explica os conceitos de base (lakehouse, Medallion, batch × streaming, Lambda × Kappa,
# > ETL × ELT) e mostra o estado real do lakehouse local agora.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Arquitetura e desenvolvimento de pipelines | §3 a §8, ADRs (§9) |
# | Definição de arquiteturas e evolução de soluções (diferencial) | §3, §5 (críticas ao Medallion), §9 |
# | Boa comunicação, equipes multidisciplinares | §2 (roteiros de demonstração), ADRs |
# | Databricks · Azure | §3 (arquitetura alvo), §10 (o que existe local) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup

# %%
import json
import re
import shutil
import time
from pathlib import Path

from pyspark.sql import functions as F

from oss_lakehouse.bronze import BRONZE_TABLE, ingest_gharchive_bronze
from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.spark import get_spark

settings = get_settings()
DATA = Path(settings.data_root)
DEMO = DATA / "demo" / "00"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True, exist_ok=True)

spark = get_spark("00")
print(f"Spark {spark.version} | raiz dos dados: {DATA.relative_to(PROJECT_ROOT)}/")

# %% [markdown]
# ## 1. Mapa de competências → onde cada uma é demonstrada 🧪
#
# **O que é** — A tabela de rastreabilidade (*traceability matrix*) entre o que se espera de um engenheiro de
# dados sênior e a evidência neste repositório. Cada linha aponta para o notebook (trilha completa em `docs/plano_notebooks.md`).
#
# **Por que importa** — Numa entrevista, "tenho experiência com X" vale pouco; "abre o notebook 09, a célula
# mostra o `explain()` antes e depois do broadcast" vale muito. A tabela é o índice para responder com prova.
#
# | Competência | Notebook(s) e tema | O que mostrar |
# |---|---|---|
# | Engenharia de dados de ponta a ponta | 03 ingestão de arquivos · 04 API incremental · 05 MERGE/SCD2 · 07 modelagem · 17 system design | pipeline de ponta a ponta, idempotente, com dado atrasado e duplicado |
# | Python avançado | 02 Python avançado · `src/oss_lakehouse/` · `tests/` | typing, generators, decorators (`utils/retry.py`), pydantic, pytest |
# | Databricks e processamento de dados | 01 ambiente · 03 Auto Loader · 08 Lakeflow/SDP · 09 performance · 10 Delta por dentro | plano físico, skew, AQE, transaction log, time travel |
# | Microsoft Azure | 01 §8 Azure Databricks · 14 Terraform + Azurite · §3 deste notebook | ADLS Gen2, Access Connector, Key Vault, VNet injection |
# | Git e versionamento | 13 Git, CI/CD e bundles | trunk-based, Conventional Commits, SemVer de pacote e de contrato, revert × reset |
# | Arquitetura e desenvolvimento de pipelines | 00 (este) · 05 · 06 streaming · 08 qualidade · ADRs | Medallion, contratos, quarentena, decisões registradas |
# | IA aplicada à engenharia de dados | 12 IA aplicada · ADR 0006 | LLM para PII/classificação/regras, **com avaliação** e humano no circuito |
# | Comunicação, multidisciplinar, ágil | §2 roteiros · ADRs · 13 PR e revisão · 17 simulado | explicar decisão com contexto e alternativas |
# | Ferramentas de IA para engenharia de dados | 12 · 13 (IA no fluxo de desenvolvimento) | avaliação, custo, rastreabilidade |
# | Projetos de engenharia com Databricks | 03 · 08 · 11 Unity Catalog · 13 bundles · 15 observabilidade | job multi-tarefa, system tables, governança |
# | Definição de arquitetura e evolução | 00 §3–§8 · ADRs · 17 | trade-offs explícitos, o que muda quando o volume cresce |
# | Projetos ágeis | 13 (trunk-based, PR pequeno, CI) · 17 | entrega incremental com teste e deploy automatizado |
# | Governança e LGPD | 11 Unity Catalog e LGPD | grants, row filter, column mask, lineage |
# | Live coding | 16 exercícios SQL/PySpark | janelas, dedup, top-N, sessionização |
#
# **Como funciona** — A célula abaixo lê o plano da trilha e confere quais notebooks já existem como fonte e como
# `.ipynb` executado (o repositório é construído em paralelo; o que falta aparece como pendente).

# %%
plano = (PROJECT_ROOT / "docs" / "plano_notebooks.md").read_text(encoding="utf-8")
linhas = re.findall(r"^\| (\d\d) \| `([^`]+)` \| (.+?) \|$", plano, flags=re.M)
src_dir, nb_dir = PROJECT_ROOT / "notebooks" / "_src", PROJECT_ROOT / "notebooks"
prontos = 0
for num, arquivo, tema in linhas:
    tem_src = (src_dir / arquivo).exists()
    tem_nb = (nb_dir / arquivo.replace(".py", ".ipynb")).exists()
    prontos += tem_nb
    status = "executado" if tem_nb else ("fonte" if tem_src else "pendente")
    print(f"{num}  {status:<9}  {tema[:88]}")
print(f"\n{len(linhas)} notebooks na trilha; {prontos} com .ipynb executado neste momento")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Organizei o repositório como prova por competência: cada notebook cobre um
# > tema com código executado e saída versionada — ingestão incremental, MERGE e SCD2, streaming, performance,
# > Delta por dentro, governança, IA com avaliação, CI/CD com bundles e Terraform da Azure. Posso abrir qualquer
# > um e rodar ao vivo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que dados públicos do GitHub:** volume real (~92 mil eventos/hora), JSON semiestruturado com formato
#   variável por tipo, duplicatas entre arquivos, skew natural (bots) e uma fonte de streaming de verdade
#   (Wikimedia) — os problemas de produção aparecem sem inventar dado.
# - **Por que notebooks *e* pacote:** a lógica fica em `src/` com teste; o notebook conta a história e chama o
#   pacote. É o mesmo wheel que o job do Databricks executa (notebook 13).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Um repositório de demonstração cobre **amplitude**; profundidade de produção (SLAs reais, on-call, custo de
#   meses) se conta com experiência, não com notebook.

# %% [markdown]
# ## 2. Como usar este repositório: estudar e demonstrar ao vivo 🧪
#
# **Para estudar** — siga a trilha na ordem; cada notebook termina com perguntas de entrevista e um resumo de
# 3–5 bullets para a véspera. Preparação única (offline depois disso):
#
# ```bash
# make setup && make data && make bronze   # dependências, GH Archive de 2026-10-01, bronze
# make nb N=05                             # regera e executa um notebook
# make test                                # testes (Spark local)
# ```
#
# **Roteiro de 10 minutos** (entrevista técnica curta — 1 história, ponta a ponta):
#
# | Min | Abrir | O que falar |
# |---|---|---|
# | 0–2 | 00 §3 (diagrama) | fontes → landing → bronze/silver/gold → consumo; por que lakehouse e Delta (ADRs 0001–0002) |
# | 2–4 | 03 | ingestão incremental com checkpoint: rodar de novo processa **0** linhas (idempotência); Auto Loader no Databricks |
# | 4–6 | 05 | MERGE com dedup e SCD2: chave, dado atrasado, por que o MERGE é idempotente |
# | 6–8 | 13 | `databricks.yml` + job bronze → silver → gold → quality; CI com testes e deploy por ambiente |
# | 8–10 | 12 | IA no pipeline com avaliação (golden set, métrica, limiar) e humano no circuito |
#
# **Roteiro de 30 minutos** (painel técnico — mostra profundidade):
#
# | Min | Abrir | O que falar |
# |---|---|---|
# | 0–3 | 00 §3 e §5 | arquitetura alvo × local; Medallion e suas críticas |
# | 3–8 | 03 + 04 | arquivo (Auto Loader, schema evolution, rescued data) e API (paginação, rate limit, ETag, marca d'água) |
# | 8–13 | 05 + 07 | silver com MERGE/SCD2; gold dimensional (star schema, Liquid Clustering) |
# | 13–18 | 09 | `explain()` antes/depois: broadcast, skew com AQE e salting, small files |
# | 18–21 | 10 | transaction log aberto no disco, time travel, RESTORE, VACUUM |
# | 21–24 | 08 + 11 | expectations e quarentena; Unity Catalog, row filter e column mask (LGPD) |
# | 24–27 | 13 + 14 | bundle, GitHub Actions com OIDC, Terraform da Azure |
# | 27–30 | 17 | system design: "como isso escala para 100×?" e troubleshooting |
#
# **Como funciona** — Antes da entrevista: `make nb N=03` para garantir que roda nesta máquina e abrir os
# `.ipynb` já executados (as saídas estão no Git — se a demo ao vivo falhar, a evidência continua lá).

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Se tivermos 10 minutos, eu mostro uma história só: arquivo chegando, bronze
# > incremental, silver com MERGE, job no Databricks via bundle e IA com avaliação. Com 30, entro em performance,
# > Delta por dentro, governança e infraestrutura."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Plano B de demonstração:** os `.ipynb` versionados já têm a saída; a demo ao vivo é bônus, não dependência.
# - **Pergunta de entrevistador não técnico:** use a §3 (diagrama) e o resumo de cada notebook, sem código.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Demonstração ao vivo consome tempo de conversa; se o entrevistador quer discutir decisões, mostre o ADR e não
#   o código.

# %% [markdown]
# ## 3. Arquitetura alvo na Azure e a equivalente local ☁️/🧪
#
# **O que é** — O desenho de referência do pipeline em produção (Azure Databricks) e o mapeamento de cada peça
# para o que roda no laptop.
#
# **Por que importa** — Mostra que o projeto local não é brinquedo: cada componente tem um par em produção, e o
# que muda entre os dois é configuração (caminho, sessão), não código (ADR 0005).
#
# **Como funciona** — Arquitetura alvo:
#
# > 📐 O visualizador de notebooks do GitHub não renderiza Mermaid — diagrama renderizado: [docs/diagramas.md](https://github.com/alanjoffre/oss-lakehouse/blob/main/docs/diagramas.md#nb00-1)
#
# ```mermaid
# flowchart LR
#     subgraph SRC["Fontes"]
#         GHA["GH Archive<br/>1 arquivo JSON.gz por hora"]
#         API["API REST do GitHub"]
#         WM["Wikimedia EventStreams<br/>(SSE)"]
#     end
#     subgraph LAKE["ADLS Gen2 · Delta Lake · governado pelo Unity Catalog"]
#         LAND["landing<br/>(Volume)"]
#         BR["bronze<br/>bruto + linhagem"]
#         SI["silver<br/>limpo, dedup, SCD2"]
#         GO["gold<br/>fatos e dimensões"]
#         QU["quarentena"]
#     end
#     GHA -- "task de download" --> LAND
#     API -- "task Python<br/>token no Key Vault" --> LAND
#     WM -- "Event Hubs<br/>(protocolo Kafka)" --> BR
#     LAND -- "Auto Loader" --> BR
#     BR -- "MERGE" --> SI
#     SI -. "reprovado" .-> QU
#     SI --> GO
#     GO --> SQLW["SQL warehouse<br/>Power BI · Genie"]
#     GO --> IA["IA: AI Functions<br/>Vector Search · Model Serving"]
#     ORQ["Lakeflow Jobs<br/>bronze → silver → gold → quality"] -. "orquestra" .-> LAKE
#     CICD["GitHub Actions<br/>+ Declarative Automation Bundles"] -. "deploy" .-> ORQ
#     TF["Terraform<br/>workspace, ADLS, Access Connector, Key Vault"] -. "provisiona" .-> LAKE
#     MON["Monitoramento<br/>system tables · alertas · Azure Monitor"] -. "observa" .-> ORQ
# ```
#
# Equivalente local (o que substitui o quê):
#
# | Peça na Azure | Local neste repo | Notebook |
# |---|---|---|
# | ADLS Gen2 (`abfss://`) | pasta `data/` (e Azurite, emulador do Blob Storage) | 01, 14 |
# | Unity Catalog (catálogo, grants, lineage) | caminhos + `docs/contratos_de_tabelas.md` | 11 |
# | Volume de landing + Auto Loader | `data/landing/` + file source do Structured Streaming com `availableNow` | 03 |
# | Event Hubs | leitura direta do SSE da Wikimedia / amostra gravada | 06 |
# | Lakeflow Jobs | `Makefile` e `python -m oss_lakehouse.cli` | 13 |
# | Key Vault + secret scope | variáveis `OSSLH_*` / `.env` (fora do Git) | 01 |
# | SQL warehouse / Power BI | Spark SQL no notebook | 07 |
# | GitHub Actions + bundles | `pre-commit` + `make lint test` (o CI roda os mesmos comandos) | 13 |
# | Terraform aplicado | `terraform validate` sem credenciais | 14 |
# | System tables, Azure Monitor | métricas do `StreamingQuery` e API REST da Spark UI | 15 |

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "As fontes caem numa landing no ADLS; Auto Loader leva para a bronze em Delta;
# > MERGE para a silver; modelo dimensional na gold; consumo por SQL warehouse e IA. Tudo governado pelo Unity
# > Catalog, orquestrado por Lakeflow Jobs, deployado por bundle no GitHub Actions e provisionado por Terraform.
# > Local, cada peça tem um substituto e o código é o mesmo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que landing separada da bronze:** a landing guarda o arquivo como chegou (reprocessável, auditável);
#   a bronze já é tabela. Apagar a bronze e recriar a partir da landing é o *disaster recovery* mais simples.
# - **Streaming direto na bronze:** a Wikimedia entra por Event Hubs e Structured Streaming sem passar por
#   arquivo — latência de segundos; o Event Hubs guarda o log para replay (retenção configurável).
# - **Um storage account por ambiente** (dev/prod) e um container por camada ou por domínio: isolamento de
#   permissão e de custo.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Para volumes pequenos (GBs, poucos usuários), um banco gerenciado (Azure SQL/PostgreSQL) + dbt pode ser mais
#   barato e simples que um lakehouse inteiro.
# - Event Hubs só se justifica para fonte de streaming; arquivo horário vai direto para a landing.

# %% [markdown]
# ## 4. Lakehouse × Data Warehouse × Data Lake 🧪
#
# **O que é**
#
# | | Data Warehouse | Data Lake | Lakehouse |
# |---|---|---|---|
# | Armazenamento | proprietário, acoplado ao motor | arquivos abertos em object storage | arquivos abertos (Parquet) + **log transacional** (Delta/Iceberg) |
# | Schema | *schema-on-write* (definido antes de gravar) | *schema-on-read* (interpretado na leitura) | os dois: bronze flexível, silver/gold com contrato |
# | ACID, UPDATE/MERGE | sim | não | sim |
# | Dado semiestruturado, ML | limitado / caro | sim | sim |
# | Custo de storage | alto | baixo | baixo |
# | Exemplos | Synapse dedicated, Snowflake, BigQuery | Parquet/CSV no ADLS/S3 | Databricks, Fabric OneLake, Iceberg + Trino |
#
# **Por que importa** — A pergunta de fundo é "uma cópia ou duas?". Lake + warehouse = duas cópias, dois controles
# de acesso, ETL entre eles. O lakehouse tenta servir BI e ML da mesma cópia.
#
# **Como funciona** — O que transforma uma pasta de Parquet num lakehouse é o **transaction log**: cada escrita
# é um commit atômico num arquivo JSON numerado. A prova, na bronze local:

# %%
bronze_path = Path(settings.path("bronze", BRONZE_TABLE))
log_dir = bronze_path / "_delta_log"
if log_dir.exists():
    commits = sorted(log_dir.glob("*.json"))
    parquets = list(bronze_path.rglob("*.parquet"))
    print(f"bronze/{BRONZE_TABLE}: {len(parquets)} arquivos Parquet, {len(commits)} commit(s) no _delta_log")
    acoes = [json.loads(linha) for linha in commits[0].read_text().splitlines()]
    print("ações do commit 0:", [next(iter(a)) for a in acoes])
    add = next(a["add"] for a in acoes if "add" in a)
    print("um 'add' traz caminho, tamanho e estatísticas:",
          {k: add[k] for k in ("path", "size")}, "| numRecords =", json.loads(add["stats"])["numRecords"])
else:
    print("bronze ainda não existe — rode `make bronze`")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Data lake é storage barato sem garantias; warehouse é SQL com garantias e
# > formato fechado; lakehouse põe um log transacional sobre arquivos abertos e ganha ACID, MERGE e time travel
# > sem sair do object storage — BI e ML na mesma cópia."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Leitura consistente:** o leitor lista o `_delta_log`, não a pasta; arquivo Parquet escrito por um job que
#   falhou nunca entrou num commit e é invisível (e removido pelo VACUUM). Detalhe no notebook 10.
# - **Warehouse moderno também separa storage e compute** (Snowflake, BigQuery); a diferença que sobra é o
#   formato aberto e o acesso direto por qualquer motor — Iceberg está fechando essa distância.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Concorrência alta de escritas pequenas (OLTP) não é caso de lakehouse — é banco transacional.
# - BI com centenas de usuários e latência de milissegundos ainda pede camada de serving (SQL warehouse com
#   cache, cubo, ou o próprio warehouse).

# %% [markdown]
# ## 5. Arquitetura Medallion — e as críticas a ela 🧪
#
# **O que é** — Padrão de organização em três camadas de **qualidade crescente** (ADR 0001):
#
# | Camada | Entra | Não entra | Quem lê |
# |---|---|---|---|
# | **Bronze** | dado como veio, *append-only*, com linhagem | interpretação, filtro, dedup | só a silver (e reprocessamento) |
# | **Silver** | tipado, limpo, deduplicado, com chave, histórico (SCD2), conformado entre fontes | agregação para caso de uso | analistas, cientistas, a gold |
# | **Gold** | modelo de consumo: fatos/dimensões, agregados, features | dado sem dono ou sem contrato | BI, aplicações, IA |
#
# **Por que importa** — Separa *ingerir* de *interpretar*: uma regra de negócio errada na silver se corrige
# reprocessando a bronze, sem voltar à fonte (que pode nem ter mais o dado).
#
# **Como funciona** — A bronze guarda o `payload` como JSON bruto (ADR 0003). A prova de que interpretar na
# bronze seria frágil: o conjunto de chaves do payload **muda por tipo de evento**.

# %%
if log_dir.exists():
    bronze = spark.read.format("delta").load(str(bronze_path))
    chaves = (bronze.select("type", F.array_sort(F.json_object_keys("payload")).alias("chaves"))
              .groupBy("type").agg(F.count("*").alias("eventos"),
                                   F.count_distinct("chaves").alias("formatos_de_payload"),
                                   F.first("chaves").alias("exemplo_de_chaves")))
    chaves.orderBy(F.desc("eventos")).show(6, truncate=70)
else:
    print("bronze ainda não existe — rode `make bronze`")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Bronze é a cópia fiel e reprocessável; silver é a verdade limpa por entidade, com
# > dedup e histórico; gold é o modelo de consumo. O ganho é separar ingestão de interpretação. Mas Medallion é
# > critério de qualidade, não de modelagem — a gold continua precisando de Kimball."
#
# <details><summary>🔎 Se o entrevistador cavar mais — as críticas ao Medallion</summary>
#
# - **Não é modelagem:** diz *quão confiável* é a tabela, não *como* modelá-la. Gold sem modelo dimensional vira
#   uma tabela por dashboard (notebook 07).
# - **Três cópias:** storage e compute para manter bronze, silver e gold. Nem toda fonte precisa das três —
#   uma tabela de referência pequena pode ir direto para a silver.
# - **Nomes vagos:** "silver" significa coisas diferentes em cada empresa; o contrato (colunas, chave, grão, dono)
#   importa mais que a cor. Variações: *raw/curated/serving*, *staging/intermediate/marts* (dbt).
# - **Latência por salto:** cada camada soma minutos; para tempo real, streaming fim a fim ou tabelas
#   materializadas incrementais (Lakeflow Declarative Pipelines).
# - **Domínio > camada em escala:** com muitos times, organiza-se por domínio (data mesh / data products), e o
#   Medallion vive dentro de cada domínio.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Camadas extras ("silver2", "gold_final") são sinal de modelagem mal resolvida, não de rigor.
# - Fonte já limpa e tipada (ex.: CDC de um banco relacional) pode ter bronze fina e silver quase igual — ok.

# %% [markdown]
# ## 6. Batch × micro-batch × streaming 🧪
#
# **O que é**
#
# | | Batch | Micro-batch | Streaming contínuo |
# |---|---|---|---|
# | Unidade | lote inteiro (ex.: o dia) | pequenos lotes incrementais | evento a evento |
# | Latência típica | horas | segundos a minutos | milissegundos |
# | Estado | recalculado | checkpoint entre lotes | checkpoint contínuo |
# | No Spark | `spark.read` / `write` | Structured Streaming (`trigger(processingTime=…)` ou `availableNow`) | *continuous processing* (experimental); no Databricks, o *real-time mode* |
#
# **Por que importa** — A latência exigida define custo e complexidade. A maioria dos pipelines analíticos não
# precisa de segundos: **incremental agendado** (micro-batch disparado de hora em hora) dá o melhor dos dois.
#
# **Como funciona** — O Structured Streaming com `trigger(availableNow=True)` processa só o que é novo e para:
# é streaming usado como batch incremental. A prova, com a amostra de 2.000 eventos dos testes, em
# `data/demo/00/` — a 1ª execução lê tudo; a 2ª, nada.

# %%
amostra = PROJECT_ROOT / "tests" / "fixtures" / "gharchive"
alvo, ckpt = str(DEMO / "bronze_demo"), str(DEMO / "_ckpt")
for rodada in (1, 2):
    t0 = time.perf_counter()
    q = ingest_gharchive_bronze(spark, str(amostra), alvo, ckpt)
    lidas = sum(p["numInputRows"] for p in q.recentProgress)
    total = spark.read.format("delta").load(alvo).count()
    print(f"rodada {rodada}: micro-batches={len(q.recentProgress)}  linhas novas lidas={lidas:>5}  "
          f"total na tabela={total}  ({time.perf_counter() - t0:.1f}s)")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Escolho pela latência que o negócio precisa, não pela moda. Para analytics, uso
# > Structured Streaming com `availableNow` agendado: o mesmo código é batch incremental hoje e vira streaming
# > contínuo amanhã trocando só o trigger, e o checkpoint garante que cada arquivo entra uma vez."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Exatamente uma vez** (*exactly-once*): fonte reprocessável + checkpoint + sink idempotente (Delta grava o
#   id do lote no commit). Notebook 06.
# - **Custo do contínuo:** cluster ligado 24 h. Com `availableNow`, o cluster sobe, processa e desliga.
# - **Watermark** (marca d'água): quanto atraso aceitar antes de fechar uma janela — só existe com estado
#   (agregações, joins de streams). Notebook 06.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Streaming para relatório diário é custo e complexidade sem benefício.
# - Batch "full reload" é aceitável para tabelas pequenas e simplifica muito (sem checkpoint, sem estado).

# %% [markdown]
# ## 7. Lambda × Kappa 🧪
#
# **O que é** — Duas arquiteturas para combinar histórico e tempo real.
#
# - **Lambda:** duas trilhas — *batch layer* (recalcula tudo, correto e lento) e *speed layer* (streaming,
#   rápido e aproximado) — e uma *serving layer* que junta as duas.
# - **Kappa:** uma trilha só, de streaming, sobre um **log reprocessável** (Kafka/Event Hubs, ou uma tabela
#   Delta); reprocessar = reler o log do início com o código novo.
#
# > 📐 O visualizador de notebooks do GitHub não renderiza Mermaid — diagrama renderizado: [docs/diagramas.md](https://github.com/alanjoffre/oss-lakehouse/blob/main/docs/diagramas.md#nb00-2)
#
# ```mermaid
# flowchart LR
#     subgraph Lambda
#         F1["fonte"] --> B["batch layer"] --> SV["serving<br/>(junta as duas)"]
#         F1 --> SP["speed layer"] --> SV
#     end
#     subgraph Kappa
#         F2["fonte"] --> LOG["log reprocessável<br/>(Event Hubs / Delta)"] --> ST["streaming"] --> OUT["tabela"]
#     end
# ```
#
# **Por que importa** — A Lambda obriga a escrever a mesma regra duas vezes (em dois motores) e a reconciliar
# os resultados — fonte eterna de divergência. A Kappa tem um código só.
#
# **Como funciona** — No lakehouse, a separação se dissolve: a tabela Delta é ao mesmo tempo tabela de batch e
# fonte de streaming (o próprio log). O mesmo DataFrame lê em batch ou em streaming:

# %%
batch = spark.read.format("delta").load(alvo)
stream = spark.readStream.format("delta").load(alvo)
print(f"mesma tabela, mesmo schema: {batch.schema == stream.schema} | batch.isStreaming={batch.isStreaming} "
      f"| stream.isStreaming={stream.isStreaming}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Lambda tem duas implementações da mesma regra e alguém para reconciliar; Kappa tem
# > uma, sobre um log reprocessável. Com Delta, a tabela é o log: leio em batch ou em streaming com a mesma API,
# > então na prática faço Kappa — e o reprocessamento é reler a bronze."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Quando Lambda ainda aparece:** o streaming aproxima (ex.: contagem distinta aproximada) e um batch noturno
#   corrige; ou sistemas legados de batch convivendo com um streaming novo.
# - **Limite da Kappa:** reprocessar anos de eventos por streaming pode ser lento; na prática faz-se *backfill*
#   em batch lendo o mesmo log.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Kappa exige log com retenção suficiente para o reprocessamento (Event Hubs tem retenção limitada; a bronze
#   Delta resolve isso).

# %% [markdown]
# ## 8. ETL × ELT 🧪
#
# **O que é** — **ETL** (*extract, transform, load*): transforma antes de gravar no destino (ferramenta de
# integração no meio). **ELT** (*extract, load, transform*): grava o bruto primeiro e transforma **dentro** da
# plataforma (SQL/Spark), em camadas.
#
# **Por que importa** — ELT preserva o bruto (reprocessável, auditável) e usa o motor escalável do destino; ETL
# reduz volume e protege o destino de dado sensível.
#
# **Como funciona** — Este projeto é ELT: o bruto entra na bronze (L) e o T acontece na silver. A prova é que o
# `payload` está gravado como texto e só vira estrutura quando alguém interpreta:

# %%
if log_dir.exists():
    (bronze.where("type = 'PushEvent'")
     .select(F.length("payload").alias("bytes_payload"),
             F.get_json_object("payload", "$.ref").alias("ref"),
             F.get_json_object("payload", "$.head").substr(1, 12).alias("head"))
     .show(3, truncate=False))
    print(f"tipo de 'payload' na bronze: {dict(bronze.dtypes)['payload']}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Prefiro ELT: carrego o bruto e transformo dentro do lakehouse em camadas, porque o
# > bruto fica reprocessável e o Spark escala. A exceção é dado sensível: PII que não deve nem entrar no lake é
# > mascarada ou descartada na extração — isso é um 'T' antes do 'L', e está certo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **EtLT:** um "t" leve na ingestão (padronizar encoding, remover PII, adicionar linhagem) e o "T" pesado
#   depois — é o que a bronze deste repo faz (`_source_file`, `_ingested_at`).
# - **Ferramentas:** ADF/Fivetran/Lakeflow Connect fazem o E+L; dbt/Spark/SDP fazem o T.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - ELT guarda mais dado (custo, superfície de vazamento). Com LGPD, "guardar tudo" precisa de retenção e
#   mascaramento (notebook 11).

# %% [markdown]
# ## 9. Decisões registradas: ADRs 🧪
#
# **O que é** — *Architecture Decision Records* (registros de decisão de arquitetura): um arquivo curto por
# decisão, com **contexto → decisão → alternativas → consequências**, em `docs/adr/`.
#
# **Por que importa** — Arquitetura é a soma das decisões difíceis de desfazer. Sem registro, cada pessoa nova
# reabre a discussão, ou pior, desfaz a decisão sem saber o motivo. É também a melhor ferramenta de comunicação
# com times multidisciplinares: o *porquê* fica escrito.
#
# **Como funciona**

# %%
for adr in sorted((PROJECT_ROOT / "docs" / "adr").glob("0*.md")):
    texto = adr.read_text(encoding="utf-8")
    titulo = texto.splitlines()[0].removeprefix("# ")
    status = re.search(r"\*\*Status:\*\*\s*(\w+)", texto).group(1)
    print(f"{titulo:<70} [{status}]")

# %% [markdown]
# | ADR | Decisão em uma linha |
# |---|---|
# | 0001 | Lakehouse com Medallion: uma cópia do dado para BI e ML, camadas por qualidade |
# | 0002 | Delta Lake (× Iceberg, Hudi): nativo no Databricks; UniForm deixa a porta do Iceberg aberta |
# | 0003 | `payload` como JSON bruto na bronze: a bronze nunca quebra por mudança da fonte |
# | 0004 | Lakeflow Jobs (× ADF, Airflow): tudo roda no Databricks; ADF só se houver fonte on-premises |
# | 0005 | Local-first com paridade por configuração; o que não existe local é marcado ☁️ |
# | 0006 | IA no pipeline só com avaliação, saída validada e humano no circuito |
#
# > 🎤 **Resposta de 30 s:** "Registro decisão de arquitetura em ADR: contexto, decisão, alternativas e
# > consequências, inclusive as ruins. Quando o contexto muda, escrevo um ADR novo que substitui o antigo — o
# > histórico mostra por que o sistema é como é."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **O que vira ADR:** decisão cara de reverter (formato de tabela, orquestrador, modelo de segurança). Escolha de
#   biblioteca de teste não vira.
# - **ADR é revisado em PR** como código — o time comenta antes de aceitar.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - ADR demais vira burocracia; ADR desatualizado engana. Imutável + "substituído por" resolve o segundo.

# %% [markdown]
# ## 10. Estado atual do lakehouse local 🧪
#
# **O que é** — Inventário do que existe agora em `data/`: arquivos de entrada e tabelas Delta por camada, com
# linhas, arquivos, tamanho e versão.
#
# **Por que importa** — Antes de qualquer demonstração, saber o que está carregado. Em produção, o equivalente é
# consultar o Unity Catalog e as system tables.
#
# **Como funciona** — Uma tabela Delta é uma pasta com `_delta_log/`. A célula é tolerante: camada que ainda não
# existe aparece como vazia (outros notebooks criam silver e gold).

# %%
def tamanho(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


for nome in ("landing/gharchive", "raw_cache/gharchive"):
    pasta = DATA / nome
    arquivos = sorted(pasta.glob("*.json.gz")) if pasta.exists() else []
    print(f"{nome:<22} {len(arquivos):>3} arquivos  {tamanho(pasta) / 1e6 if pasta.exists() else 0:>8.1f} MB")

print()
from delta.tables import DeltaTable  # noqa: E402

for camada in ("bronze", "silver", "gold", "quarantine"):
    raiz = DATA / camada
    tabelas = sorted({p.parent for p in raiz.rglob("_delta_log")}) if raiz.exists() else []
    if not tabelas:
        print(f"{camada:<10} (nenhuma tabela ainda)")
    for t in tabelas:
        try:
            det = DeltaTable.forPath(spark, str(t)).detail().first()
            versao = DeltaTable.forPath(spark, str(t)).history(1).first()["version"]
            linhas = spark.read.format("delta").load(str(t)).count()
            part = ",".join(det["partitionColumns"]) or "-"
            print(f"{camada:<10} {str(t.relative_to(raiz)):<28} linhas={linhas:>9,}  arquivos={det['numFiles']:>4}  "
                  f"{det['sizeInBytes'] / 1e6:>7.1f} MB  versão={versao:<3} partição={part}")
        except Exception as exc:  # tabela sendo escrita por outro processo, por exemplo
            print(f"{camada:<10} {t.relative_to(raiz)}: não foi possível ler ({type(exc).__name__})")

# %% [markdown]
# O `count()` de uma tabela Delta sem filtro nem precisa ler os Parquet: o Delta responde pelas estatísticas do
# log (`numRecords` de cada arquivo). O plano físico mostra isso:

# %%
if log_dir.exists():
    plano_count = spark.sql(f"SELECT COUNT(*) FROM delta.`{bronze_path}`")._jdf.queryExecution().executedPlan().toString()
    print(plano_count.strip()[:400])

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Antes de demonstrar, confiro o inventário: a landing com as horas do GH Archive,
# > a bronze com as linhas, arquivos e versão. Em Delta, `count(*)` sem filtro sai das estatísticas do log —
# > é metadado, não varredura."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Small files:** muitos arquivos pequenos por tabela/partição custam abertura de arquivo e listagem;
#   `numFiles` e `sizeInBytes` do `DESCRIBE DETAIL` são o primeiro termômetro (notebook 09).
# - **No Databricks:** `DESCRIBE DETAIL catalogo.schema.tabela`, `information_schema.tables` e
#   `system.storage.*`/`system.billing.usage` para custo por tabela/job (notebook 15).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Varrer pastas para descobrir tabelas só funciona localmente; em produção, a fonte da verdade é o catálogo.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```sql
# -- Inventário pelo Unity Catalog (substitui a varredura de pastas da §10)
# SELECT table_catalog, table_schema, table_name, table_type, data_source_format
# FROM system.information_schema.tables
# WHERE table_catalog = 'oss_prod' AND table_schema IN ('bronze', 'silver', 'gold');
#
# DESCRIBE DETAIL oss_prod.bronze.gh_events;      -- numFiles, sizeInBytes, clusteringColumns
# DESCRIBE HISTORY oss_prod.bronze.gh_events;     -- quem escreveu, quando, com qual operação
# ```
#
# ```python
# # Bronze por Auto Loader a partir do Volume de landing (notebook 03)
# (spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", "/Volumes/oss_prod/bronze/_schemas/gh_events")
#     .schema(GH_EVENT_SCHEMA)
#     .load("/Volumes/oss_prod/landing/gharchive/")
#  .writeStream.option("checkpointLocation", "/Volumes/oss_prod/bronze/_checkpoints/gh_events")
#     .trigger(availableNow=True)
#     .toTable("oss_prod.bronze.gh_events"))
# ```

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. O que é um lakehouse e em que difere de um data warehouse?**
# <details><summary>Resposta</summary>
# Tabelas transacionais (log tipo Delta/Iceberg) sobre arquivos abertos em object storage: ACID, MERGE e time
# travel com storage barato e acesso por vários motores. O warehouse tem formato fechado e acoplado ao motor;
# serve SQL muito bem, mas ML e semiestruturado custam mais e costumam exigir uma segunda cópia.
# </details>
#
# **2. O que entra em cada camada do Medallion?**
# <details><summary>Resposta</summary>
# Bronze: bruto, append-only, com linhagem. Silver: tipado, deduplicado, com chave e histórico, conformado.
# Gold: modelo de consumo (fatos/dimensões, agregados). Quarentena para o reprovado.
# </details>
#
# **3. Quais as críticas ao Medallion?**
# <details><summary>Resposta</summary>
# Não é modelagem; triplica storage/compute; nomes vagos sem contrato; latência por salto; em escala, domínio
# importa mais que camada. Contrato por tabela resolve mais que a cor da camada.
# </details>
#
# **4. Por que guardar o payload bruto na bronze em vez de inferir o schema?**
# <details><summary>Resposta</summary>
# O formato muda por tipo e ao longo do tempo; inferência quebra ou cria colunas esparsas. Bruto + schema
# explícito na silver = bronze estável e reprocessável (ADR 0003). No Databricks, VARIANT é a evolução.
# </details>
#
# **5. Batch, micro-batch ou streaming: como você decide?**
# <details><summary>Resposta</summary>
# Pela latência exigida e pelo custo. Analytics de hora em hora: Structured Streaming com `availableNow` agendado.
# Segundos: micro-batch contínuo. Milissegundos: motor de streaming de baixa latência (real-time mode, Flink).
# </details>
#
# **6. Lambda ou Kappa?**
# <details><summary>Resposta</summary>
# Kappa quando há log reprocessável — um código só. Lambda duplica regra em dois motores. Com Delta, a tabela é o
# log, então o lakehouse tende naturalmente para Kappa.
# </details>
#
# **7. ETL ou ELT?**
# <details><summary>Resposta</summary>
# ELT por padrão (bruto preservado, transformação escalável no destino); "T" antes do "L" para PII e redução de
# volume — EtLT.
# </details>
#
# **8. Delta, Iceberg ou Hudi?**
# <details><summary>Resposta</summary>
# Delta no Databricks (nativo, Photon, Liquid, Predictive Optimization); Iceberg quando vários motores de
# fornecedores diferentes escrevem a mesma tabela; Hudi para upsert/CDC intenso com índice. UniForm reduz o
# custo da escolha.
# </details>
#
# **9. Lakeflow Jobs, ADF ou Airflow?**
# <details><summary>Resposta</summary>
# Jobs quando tudo roda no Databricks (zero infra extra, bundle, system tables). ADF para cópia de fontes
# on-premises/SaaS. Airflow quando se orquestra muitos sistemas heterogêneos e se aceita operar o Airflow.
# </details>
#
# **10. O que é um ADR e o que vai nele?**
# <details><summary>Resposta</summary>
# Registro curto de uma decisão de arquitetura: contexto, decisão, alternativas, consequências (incluindo as
# ruins). Imutável; mudança vira ADR novo que substitui o anterior.
# </details>
#
# **11. Como você levaria este pipeline do laptop para a Azure?**
# <details><summary>Resposta</summary>
# Terraform para workspace, ADLS, Access Connector e Key Vault; Unity Catalog com external locations; o mesmo
# wheel executado por um Lakeflow Job definido em bundle; GitHub Actions com OIDC para deploy em dev e prod;
# só a configuração (`OSSLH_DATA_ROOT`) muda.
# </details>

# %% [markdown]
# ## Resumo
#
# - Cada competência tem um notebook com evidência executada (§1); roteiros de 10 e 30 minutos (§2).
# - Alvo: fontes → landing (ADLS) → bronze/silver/gold Delta no Unity Catalog → SQL/IA; Lakeflow Jobs, bundles,
#   Terraform, Key Vault; local, cada peça tem um substituto e o código é o mesmo.
# - Lakehouse = log transacional sobre arquivos abertos; Medallion = qualidade por camada (não é modelagem).
# - Incremental agendado (`availableNow`) cobre a maioria dos casos; Kappa sobre Delta; ELT com "t" de PII.
# - Decisões caras ficam em ADR: contexto, decisão, alternativas, consequências.

# %%
spark.stop()
