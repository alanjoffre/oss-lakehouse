# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 04 · Ingestão de API REST incremental (GitHub)
#
# > Um cliente de API que respeita paginação, cota, ETag e falhas, grava a resposta **bruta** na landing,
# > consolida com MERGE idempotente e, reexecutado, busca **só o que mudou** desde a última marca d'água.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Python avançado | cliente HTTP com `requests.Session`, retry por decorator, adapter de gravação/replay |
# | Arquitetura e desenvolvimento de pipelines | landing bruta → Silver por MERGE; estado incremental; idempotência |
# | Databricks e processamento de dados | MERGE Delta, leitura de JSON com schema explícito, Lakeflow Jobs ☁️ |
# | Microsoft Azure | token no Key Vault + secret scope ☁️, landing no ADLS ☁️ |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Modo de execução.** A API do GitHub sem token permite **60 requisições por hora por IP**. O notebook
# consulta a cota (essa consulta é gratuita) e decide: com cota suficiente roda **ao vivo**; sem rede ou sem
# cota, roda em **replay** — as respostas reais gravadas em `tests/fixtures/github_api/` (gravadas em
# 2026-10-05). O código do pipeline é o mesmo nos dois modos; só muda o *transporte* HTTP.
# Forçar: `OSSLH_GITHUB_MODE=live|replay`.

# %% [markdown]
# ## Setup

# %%
import base64
import json
import os
import shutil
import struct
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests
from pyspark.sql import functions as F

from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.sources import github_api as gh
from oss_lakehouse.spark import get_spark

s = get_settings()
spark = get_spark("04")

FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "github_api"
LANDING = s.path("landing", "github_api")  # metadados de repositórios (landing compartilhada)
SILVER_REPOS = s.path("silver", "github_repos")
DEMO = Path(s.data_root) / "demo" / "04"  # incremental de issues: recomeça do zero a cada execução
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True)

# %%
MODE = os.environ.get("OSSLH_GITHUB_MODE", "auto")
BUDGET = 26  # chamadas que este notebook gasta ao vivo (18 repos + 1 condicional + até 4 páginas + folga)

if MODE == "auto":
    try:
        probe = gh.GitHubClient(token=s.github_token, attempts=1, timeout=(3, 5)).rate_limit()
        MODE = "live" if probe.remaining >= BUDGET else "replay"
        print(f"cota agora: {probe.remaining}/{probe.limit}, reset {probe.reset:%H:%M} UTC → modo {MODE}")
    except (requests.RequestException, gh.RateLimitExceeded) as exc:
        MODE = "replay"
        print(f"sem acesso à API ({type(exc).__name__}) → modo replay")

session = requests.Session() if MODE == "live" else gh.cassette_session(FIXTURES, "replay")
client = gh.client_from_settings(session=session)
RUN_ID = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") if MODE == "live" else "fixture-20261005"
print(f"modo: {MODE} | token: {'sim' if s.github_token else 'não (60 req/h por IP)'} | run_id: {RUN_ID}")

# %% [markdown]
# ## 1. Por que buscar numa API — e não em arquivo ou CDC
#
# **O que é.** Há três jeitos clássicos de trazer dado de um sistema de fora:
#
# | Padrão | Como chega | Exemplo aqui | Ponto forte | Ponto fraco |
# |---|---|---|---|---|
# | **Arquivo** (*file drop*) | o produtor larga arquivos num storage | GH Archive (1 `.json.gz` por hora) | volume alto, barato, reprocessável | latência = cadência do arquivo |
# | **API** (*pull*) | você pergunta, página por página | API REST do GitHub | dado que só existe lá (estrelas, licença, issues) | cota, paginação, falhas — tudo é problema seu |
# | **CDC** (*change data capture*) | o log de transações do banco vira eventos | Debezium/Lakeflow Connect lendo um Postgres ☁️ | captura insert/update/**delete**, sem carga no banco | precisa de acesso ao log do banco |
#
# **Por que importa.** O GH Archive diz *o que aconteceu* (eventos), mas não diz *como é* o repositório:
# estrelas, linguagem, licença, tópicos. Isso só a API tem. É o caso típico de **enriquecimento** (*enrichment*):
# a fonte principal é arquivo; a API complementa.
#
# **Como funciona.** O evento na bronze traz só `repo.id`, `repo.name` e `repo.url`. Escolhemos os
# repositórios mais ativos (mais **pessoas** distintas, bots fora) e buscamos os metadados de cada um.

# %%
bronze = spark.read.format("delta").load(s.path("bronze", "gh_events"))
print("campos de repo na bronze:", bronze.select("repo.*").columns)
REPOS = gh.select_active_repos(bronze, n=18)
print(f"{len(REPOS)} repositórios selecionados; primeiros: {REPOS[:5]}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Escolho o padrão pela fonte: arquivo quando o produtor exporta em lote, CDC quando
# > a fonte é um banco e eu preciso de deletes e baixa latência, API quando o dado só existe atrás de um endpoint.
# > API é o mais frágil — cota, paginação, falha transitória e mudança de contrato são problema meu — então
# > isolo a ingestão: gravo a resposta bruta e só depois transformo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Push × pull:** webhook (o GitHub chama você) dá latência baixa, mas você precisa de um endpoint sempre de pé
#   e de reconciliação periódica — webhook perdido não volta. Pull com marca d'água é mais simples de operar.
# - **Por que não CDC aqui:** não temos acesso ao banco do GitHub. CDC é a escolha quando o sistema é seu
#   (ex.: Postgres do ERP) — e é o único dos três que enxerga *delete* sem truque.
# - **Escala:** para milhares de repositórios a API REST vira gargalo de cota; a saída é GraphQL (vários campos
#   numa chamada), GitHub App (cota maior por instalação) ou aceitar o GH Archive como fonte primária.
# </details>
#
# **Trade-offs / quando NÃO usar API**
# - Se existe export em arquivo (dump diário, *data share*), prefira: mais barato e reprocessável.
# - API para volume alto (milhões de registros/dia) costuma estourar cota e janela de execução.
# - Dado que precisa de delete fiel → CDC, não `GET` paginado.

# %% [markdown]
# ## 2. O cliente HTTP: sessão, timeout, versão da API e token fora do código
#
# **O que é.** Um `requests.Session` reaproveita a conexão TCP/TLS (*connection pooling*) e carrega os
# headers fixos: `User-Agent` identificável (o GitHub exige), `Accept` e **`X-GitHub-Api-Version`** — a API
# é versionada por data; fixar a versão protege o pipeline de mudança de contrato.
#
# **Por que importa.** Os três bugs mais comuns de quem consome API: sem **timeout** (o job trava para sempre
# esperando um socket), segredo **no código** (vaza no Git) e sem identificar a versão da API.
#
# **Como funciona.** O token vem de `OSSLH_GITHUB_TOKEN` (variável de ambiente, padrão *12-factor*); no Databricks,
# do *secret scope* ligado ao Key Vault ☁️. O timeout é uma tupla `(conexão, leitura)`.

# %%
safe_headers = {k: ("***" if k == "Authorization" else v) for k, v in client.session.headers.items()}
print(json.dumps(safe_headers, indent=1))
print("timeout (conexão, leitura):", client.timeout, "| tentativas:", client.attempts)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Todo cliente de API que escrevo tem sessão reaproveitada, timeout explícito,
# > versão da API fixada e segredo vindo de fora — variável de ambiente local, Key Vault na Azure. Sem timeout,
# > um socket pendurado segura o job e o cluster por horas."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `requests` **não tem timeout padrão**: sem o parâmetro, espera indefinidamente.
# - O adapter (`HTTPAdapter`) é o ponto de extensão do `requests`: aqui ele grava/reproduz respostas
#   (`CassetteAdapter`); em produção pode limitar o pool de conexões.
# - Token de **GitHub App** (expira em 1 h, cota por instalação) é preferível a token pessoal em produção.
# </details>
#
# **Trade-offs**
# - Uma sessão por *thread*: `Session` não é garantidamente *thread-safe*.
# - Fixar versão da API exige revisar quando ela for descontinuada — anote no runbook.

# %% [markdown]
# ## 3. Rate limit: a cota vem em toda resposta
#
# **O que é.** *Rate limit* é o teto de requisições por janela. O GitHub informa o estado em **todo** response:
# `x-ratelimit-limit`, `x-ratelimit-remaining`, `x-ratelimit-used`, `x-ratelimit-reset` (epoch em que a cota volta).
#
# **Por que importa.** Pipeline que ignora a cota descobre o limite no meio da execução, com metade dos dados.
# Lendo os headers, o job decide antes: segue, espera o reset ou para e reagenda.
#
# **Como funciona.** `GET /rate_limit` consulta a cota **sem gastar cota**. Depois, cada resposta atualiza
# `client.last_rate_limit`. Abaixo: buscamos os metadados dos repositórios e comparamos a cota antes e depois.

# %%
before = client.rate_limit()
rep = gh.fetch_repos(client, REPOS, LANDING, RUN_ID)
after = client.last_rate_limit
print(f"status HTTP: {dict(Counter(rep.status.values()))} | chamadas feitas: {rep.calls}")
print(f"cota: {before.remaining} → {after.remaining} (gasto {before.remaining - after.remaining}); "
      f"reset {after.reset:%H:%M} UTC")
print(f"arquivos na landing: {len(rep.files)} → {rep.files[0].parent}")

# %% [markdown]
# Cada `GET /repos/{dono}/{repo}` gastou uma unidade. (Se outro processo no mesmo IP usar a API ao mesmo tempo,
# o gasto observado sobe — a cota é **por IP** sem token, e **por token** com token.)
#
# > 🎤 **Resposta de 30 s:** "Leio `x-ratelimit-remaining` e `reset` a cada resposta. Se a cota acaba e o reset
# > está perto, espero; se está longe, falho de forma controlada e o orquestrador reagenda — nunca deixo o job
# > dormindo uma hora segurando cluster."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - O GitHub tem **dois** limites: o primário (por hora) e o **secundário** (concorrência e rajada — ex.: muitas
#   requisições simultâneas ou muitos pontos por minuto). O secundário responde 403/429 com `Retry-After`.
# - Com N workers paralelos, a cota é compartilhada: limite a concorrência (semáforo) em vez de paralelizar
#   às cegas — paralelismo não aumenta cota.
# - Orçamento explícito: este notebook calcula `BUDGET` antes de começar e cai para replay se não couber.
# </details>
#
# **Trade-offs**
# - Esperar o reset dentro do job é barato só se a espera for curta (`max_wait=60 s` aqui).
# - Sem token, a cota é por IP: num cluster com IP de saída compartilhado (NAT), outros jobs gastam a sua cota.

# %% [markdown]
# ## 4. Requisição condicional com ETag (`If-None-Match` → 304)
#
# **O que é.** O **ETag** é uma "impressão digital" da versão do recurso, devolvida no header `etag`. Na próxima
# vez você manda `If-None-Match: <etag>`; se nada mudou, o servidor responde **304 Not Modified** sem corpo.
#
# **Por que importa.** Economiza banda e processamento (não há corpo para baixar nem MERGE para fazer) e, **com
# autenticação**, a documentação do GitHub diz que o 304 não desconta da cota primária.
#
# **Como funciona.** Lemos o ETag gravado no envelope da landing e repetimos a chamada condicional.

# %%
first = json.loads(rep.files[0].read_text())
name = first["data"]["full_name"]
r304 = client.get(f"repos/{name}", etag=first["_etag"])
print(f"{name}: etag {first['_etag'][:24]}… → HTTP {r304.status}, corpo: {r304.data}")
spent = after.remaining - r304.rate_limit.remaining
print(f"cota antes do 304: {after.remaining} | depois: {r304.rate_limit.remaining} | "
      f"autenticado: {'sim' if s.github_token else 'não'} → o 304 {'CONSUMIU' if spent > 0 else 'não consumiu'} cota")

# %% [markdown]
# Medido aqui: **sem token, o 304 também consumiu cota** (a cota caiu uma unidade). A isenção do 304
# vale para requisição autenticada. Moral: ETag sempre economiza transferência e processamento; economizar cota
# depende de autenticar — mais um motivo para usar token (do Key Vault) em produção.
#
# > 🎤 **Resposta de 30 s:** "Guardo o ETag de cada recurso junto com o dado bruto. Na próxima carga mando
# > `If-None-Match`; 304 significa 'nada mudou', e eu pulo download e MERGE. Com token, o GitHub nem desconta
# > o 304 da cota."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `W/"..."` é um ETag **fraco** (*weak*): equivalência semântica, não byte a byte — suficiente para cache.
# - Alternativa ao ETag: `If-Modified-Since` com o header `Last-Modified`.
# - Onde guardar o ETag em escala: numa tabela de controle (`recurso → etag, fetched_at`), não em arquivo solto.
# - ETag não ajuda em listagem incremental (issues novas): para isso serve a marca d'água (seção 7).
# </details>
#
# **Trade-offs**
# - Uma chamada por recurso continua sendo uma chamada: com 10 mil repositórios, ETag não resolve a cota sem token.
# - O ETag muda quando **qualquer** campo muda (inclusive contadores); não é "mudou o que me interessa".

# %% [markdown]
# ## 5. Retry com backoff, `Retry-After` e o limite que não vale esperar
#
# **O que é.** *Retry com backoff exponencial e jitter*: em falha **transitória** (5xx, 429, limite secundário),
# tenta de novo esperando 1 s, 2 s, 4 s… com aleatoriedade (*jitter*) para os clientes não voltarem todos juntos.
# Quando o servidor manda `Retry-After`, espera **pelo menos** o que ele pediu (o backoff soma por cima).
#
# **Por que importa.** Em pipeline diário, 502 esporádico é estatística, não incidente. Sem retry, o job cai;
# com retry ingênuo (sem espera), ele vira o próprio ataque de negação de serviço.
#
# **Como funciona.** O cliente usa o decorator `utils.retry` (o mesmo do download do GH Archive) só para erros
# retentáveis. 404 **não** é retentável: repositório apagado é *dado* (vai para o relatório). Abaixo, um servidor
# falso roteirizado — sem rede, sem espera real (o `sleep` é injetado).

# %%
from requests.adapters import HTTPAdapter  # noqa: E402  (demo: servidor falso)


class ScriptedServer(HTTPAdapter):
    """Responde uma sequência fixa de (status, headers) — simula falhas sem rede."""

    def __init__(self, script):
        super().__init__()
        self.script, self.seen = list(script), []

    def send(self, request, **kw):
        status, headers = self.script.pop(0)
        self.seen.append(status)
        resp = requests.Response()
        resp.status_code, resp.url, resp.request = status, request.url, request
        resp.headers = requests.structures.CaseInsensitiveDict(
            {"x-ratelimit-remaining": "40", "x-ratelimit-limit": "60", "x-ratelimit-reset": "0", **headers})
        resp._content = json.dumps({"id": 1} if status == 200 else {"message": "erro simulado"}).encode()
        return resp


def scripted_client(script):
    sess, slept = requests.Session(), []
    server = ScriptedServer(script)
    sess.mount("https://", server)
    return gh.GitHubClient(session=sess, sleep=slept.append), server, slept


c, server, slept = scripted_client([(502, {}), (403, {"retry-after": "3"}), (200, {})])
resp = c.get("repos/exemplo/falhas")
print(f"respostas do servidor: {server.seen} → final {resp.status}")
print("esperas (s):", [round(x, 2) for x in slept],
      "← o 3.0 é o Retry-After respeitado; as outras são o backoff com jitter (aleatório)")

far = str(int(time.time()) + 3600)
c, server, slept = scripted_client([(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": far})])
try:
    c.get("repos/exemplo/sem-cota")
except gh.RateLimitExceeded as exc:
    print(f"cota primária esgotada: {exc} — falha rápida (esperas: {len(slept)})")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Separo erro transitório de erro definitivo. 5xx, 429 e limite secundário eu retento
# > com backoff exponencial e jitter, respeitando `Retry-After`; 4xx de negócio eu não retento. Se a cota primária
# > só volta daqui a uma hora, falho rápido com uma exceção específica e deixo o orquestrador reagendar."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Full jitter** (`uniform(0, base·2^n)`): espalha as tentativas; é o que o blog de arquitetura da AWS mostrou
#   reduzir mais a contenção.
# - Retry só é seguro em operação **idempotente**. `GET` é; `POST` que cria recurso não é — aí entra
#   *idempotency key*.
# - *Circuit breaker*: depois de N falhas seguidas, para de chamar por um tempo (protege a fonte e o seu job).
# - Retry tem duas camadas: a do cliente (segundos) e a do orquestrador (Lakeflow Jobs: `max_retries` da tarefa,
#   minutos). Não empilhe as duas sem pensar — tentativas multiplicam.
# </details>
#
# **Trade-offs**
# - Retry esconde problema crônico: registre cada nova tentativa (aqui, `log.warning`) e monitore a taxa.
# - Timeout curto + retry pode duplicar carga no servidor se a primeira requisição na verdade terminou.

# %% [markdown]
# ## 6. Landing bruta → Silver com MERGE idempotente
#
# **O que é.** A **landing** guarda a resposta exatamente como veio, embrulhada num **envelope de linhagem**
# (`_request_url`, `_status`, `_fetched_at` pelo relógio do servidor, `_etag`, `_link`, `data`). A Silver `github_repos`
# tem 1 linha por repositório e é atualizada por **MERGE** (*upsert*: atualiza se existe, insere se não).
#
# **Por que importa.** Se a transformação tiver bug, reprocessa-se a landing — sem chamar a API de novo (sem cota,
# sem depender de o dado ainda existir lá). E o MERGE torna a carga **idempotente**: rodar duas vezes dá o mesmo
# resultado.
#
# **Como funciona.** Leitura com **schema explícito** (só os campos usados — campo novo na API não quebra nada),
# dedupe da captura mais recente por `repo_id` e `MERGE ... WHEN MATCHED AND s._fetched_at > t._fetched_at`
# (captura velha reprocessada não sobrescreve a nova).
#
# ```text
# API ──GET──▶ landing/github_api/repos/run=<id>/<dono>__<repo>.json   (bruto + envelope)
#                         │  schema explícito, dedupe por repo_id
#                         ▼
#              silver/github_repos  ◀── MERGE ON repo_id  (upsert, só se mais novo)
# ```

# %%
print("chaves do envelope:", list(first.keys()))
m1 = gh.merge_repos_silver(spark, LANDING, SILVER_REPOS)
m2 = gh.merge_repos_silver(spark, LANDING, SILVER_REPOS)  # reprocessa a mesma landing
keys = ["numSourceRows", "numTargetRowsInserted", "numTargetRowsUpdated"]
print("1º MERGE:", {k: m1[k] for k in keys})
print("2º MERGE:", {k: m2[k] for k in keys}, "← idempotente: nada muda")
repos = spark.read.format("delta").load(SILVER_REPOS)
print("linhas na silver.github_repos:", repos.count())

# %% [markdown]
# Na primeira execução do zero, o 1º MERGE insere tudo; em execuções seguintes ele só atualiza o que veio mais
# novo (no replay, nada — as gravações são as mesmas). O 2º MERGE nunca muda nada. `repo_id` é o mesmo `repo.id` do
# GH Archive: junta direto com a bronze, e não quebra quando o repositório é renomeado.

# %%
activity = (bronze.groupBy(F.col("repo.id").alias("repo_id"))
            .agg(F.countDistinct("actor.id").alias("pessoas_3h")))
(repos.join(activity, "repo_id")
 .select("full_name", "language", "license", "stars", "pessoas_3h")
 .orderBy(F.desc("stars")).show(5, truncate=False))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Gravo a resposta bruta com envelope — URL, status, horário do servidor, ETag — e só
# > depois transformo. A Silver é atualizada por MERGE na chave natural estável, com guarda de 'só se for mais
# > novo'. Assim reprocessar é seguro e barato: não volto à API."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Dedupe antes do MERGE**: duas linhas de origem para a mesma chave fazem o Delta falhar
#   (*multiple source rows matched*). Por isso o `row_number()` por `repo_id`.
# - **Chave estável**: `full_name` muda em rename/transferência; `id` não. Escolher a chave errada gera "duplicata"
#   que na verdade é o mesmo repositório com outro nome.
# - **Horário do servidor** (`Date`) e não do worker: relógio de máquina erra, e no replay o horário é o da gravação.
# - **SCD tipo 2** (histórico de estrelas ao longo do tempo) é o notebook 05; aqui é tipo 1 (só o estado atual).
# </details>
#
# **Trade-offs**
# - Landing bruta custa armazenamento (barato) e exige política de retenção (*lifecycle* no ADLS ☁️).
# - MERGE reescreve arquivos tocados: para milhões de linhas por carga, particione/clusterize pela chave
#   (Liquid Clustering, notebook 09) ou use *deletion vectors*.

# %% [markdown]
# ## 7. Paginação pelo header `Link` e incremental com marca d'água
#
# **O que é.**
# - **Paginação**: listas grandes vêm em páginas. O GitHub devolve o header `Link` com as URLs `next`/`prev`/`last`.
#   Siga o `next`; **não** monte `?page=N` à mão.
# - **Marca d'água** (*high-water mark*): o maior `updated_at` já ingerido, persistido num **estado**. A próxima
#   execução pede só `since=<marca>` — carga **incremental** em vez de **full** (tudo de novo).
#
# **Por que importa.** Full load de issues de um repositório grande é milhares de chamadas a cada execução;
# incremental é poucas. E paginação errada é a causa nº 1 de "sumiram registros" em ingestão de API.
#
# **Como funciona.** `sort=updated&direction=asc` + `since`: as páginas vêm em ordem crescente de atualização.
# Com o freio `max_pages=2` (orçamento de cota), a execução para no meio e a marca avança **só até onde chegou** —
# a próxima continua dali (*backfill* em fatias). A marca é gravada **depois** de todas as páginas na landing
# (ordem **dado → estado**): se cair no meio, a execução seguinte refaz o trecho, e o MERGE absorve a repetição.

# %%
ISSUES_REPO = "dust-tt/dust"  # um dos 18 mais ativos, com volume de PRs que pede paginação
state = gh.WatermarkStore(DEMO / "state" / "watermarks.json")
issues_landing = DEMO / "landing"
runs = []
for run in ("exec-1", "exec-2"):
    r = gh.fetch_issues_incremental(client, ISSUES_REPO, issues_landing, state, run,
                                    initial_since="2026-10-01T12:00:00Z", per_page=50, max_pages=2)
    runs.append(r)
    print(f"{run}: since={r.since} → {r.pages} páginas, {r.items} itens, {r.calls} chamadas; "
          f"nova marca = {r.new_watermark}")
print("estado persistido:", json.loads((DEMO / "state" / "watermarks.json").read_text()))

# %% [markdown]
# A 2ª execução começou exatamente onde a 1ª parou. Agora o header `Link` da primeira página:

# %%
page1 = json.loads(runs[0].files[0].read_text())  # o envelope guarda o header Link de cada página
nxt = gh.parse_link_header(page1["_link"])["next"]
print("next →", nxt[:95] + "…")
after = parse_qs(urlsplit(nxt).query)["after"][0]
raw = base64.b64decode(after)
ts_ms, item_id = struct.unpack(">Q", raw[12:20])[0], struct.unpack(">Q", raw[21:29])[0]
cursor_ts = datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
last = page1["data"][-1]
print(f"cursor decodificado: prefixo={raw[:10]!r} updated_at={cursor_ts} id={item_id}")
print(f"último item da página:            updated_at={last['updated_at']} id={last['id']}")

# %% [markdown]
# Duas evidências de engenharia escondidas no `Link`:
# 1. A URL troca `repos/dust-tt/dust` por `repositories/<id>` — o próximo passo usa o id estável, não o nome.
# 2. O parâmetro `after` é um **cursor** e, decodificado (formato interno, não documentado — observação nossa),
#    é o par `(updated_at, id)` do **último item da página**: paginação por **cursor/keyset**, não por deslocamento.
#    Por isso seguir o `Link` é mais seguro do que montar `?page=N`.
#
# Por fim, as páginas brutas viram uma tabela Delta de demonstração por MERGE em `id` — o item da fronteira
# (`since` é **inclusivo**: `>=`) volta na 2ª execução e é absorvido:

# %%
ISSUES_DELTA = str(DEMO / "issues_delta")
pages = (spark.read.option("multiLine", True).json(str(issues_landing / "issues" / "*" / "run=*" / "*.json"))
         .select(F.explode("data").alias("i")))
src = pages.select(F.col("i.id").alias("id"), F.col("i.number").alias("number"),
                   F.col("i.state").alias("state"), F.col("i.pull_request").isNotNull().alias("is_pr"),
                   F.to_timestamp("i.updated_at").alias("updated_at"))
fetched = src.count()
dedup = src.dropDuplicates(["id"])  # no MERGE real: row_number() pelo updated_at mais novo
from delta.tables import DeltaTable  # noqa: E402

dedup.limit(0).write.format("delta").mode("overwrite").save(ISSUES_DELTA)
(DeltaTable.forPath(spark, ISSUES_DELTA).alias("t").merge(dedup.alias("s"), "t.id = s.id")
 .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at").whenNotMatchedInsertAll().execute())
final = spark.read.format("delta").load(ISSUES_DELTA)
print(f"itens buscados: {fetched} | distintos na tabela: {final.count()} | repetidos absorvidos: {fetched - final.count()}")
final.groupBy("is_pr", "state").count().orderBy("is_pr", "state").show()

# %% [markdown]
# Repare: o endpoint `/issues` devolve **issues e pull requests** (PR tem o campo `pull_request`). Num repositório
# como este, quase tudo é PR — quem esquece o filtro conta PR como issue.
#
# > 🎤 **Resposta de 30 s:** "Incremental por marca d'água: guardo o maior `updated_at` processado e peço só
# > `since` dali, ordenado por atualização crescente. Gravo a marca só depois de o dado estar na landing, e o MERGE
# > por id absorve as repetições — `since` é inclusivo. Paginação eu sigo pelo header `Link`, que no GitHub já é
# > cursor; offset em lista que muda durante a leitura pula registro."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Offset × cursor:** com `?page=N` (offset), se um item for atualizado durante a leitura ele "pula" para o fim
#   da ordenação e todos os seguintes sobem uma posição — o item na fronteira da página some sem erro. Cursor
#   (*keyset*: "depois de `(updated_at, id)` = X") não sofre disso.
# - **Margem de segurança** (*lookback*): `fetch_issues_incremental(..., lookback=timedelta(minutes=5))` recua a
#   marca para cobrir relógio e indexação atrasada da fonte; o MERGE absorve o que repetir.
# - **Delete não aparece** em `since`: issue apagada some sem evento. Solução: reconciliação periódica (full em
#   janela larga, semanal) ou CDC/webhook.
# - **Backfill** = rodar o mesmo incremental com a marca reposicionada (ex.: `initial_since` antigo numa chave de
#   estado própria) — sem código especial. Em fatias, como aqui, para caber na cota.
# - **Estado em produção:** tabela Delta de controle (`fonte, entidade, marca, run_id, committed_at`), atualizada
#   na mesma tarefa do Lakeflow Job que gravou a landing.
# </details>
#
# **Trade-offs / quando NÃO usar incremental**
# - Fonte sem campo confiável de atualização (`updated_at`) → full load periódico com comparação (hash).
# - Tabela pequena (milhares de linhas): full é mais simples e pega deletes de graça.
# - Incremental acumula deriva: combine com reconciliação full de tempos em tempos.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# **Segredo no Key Vault, lido pelo secret scope** (o valor aparece como `[REDACTED]` se alguém imprimir):
#
# ```python
# token = dbutils.secrets.get(scope="kv", key="github-token")   # scope apoiado no Key Vault (notebook 14)
# client = GitHubClient(token=token)
# ```
#
# **Orquestração com Lakeflow Jobs** (antigo Databricks Workflows), declarado em Asset Bundle (notebook 13):
#
# ```yaml
# resources:
#   jobs:
#     github_enrichment:
#       schedule: { quartz_cron_expression: "0 15 * * * ?", timezone_id: "UTC" }   # de hora em hora
#       max_concurrent_runs: 1            # duas execuções simultâneas brigariam pela marca d'água
#       tasks:
#         - task_key: fetch_landing       # API → landing (abfss://lake@<conta>.dfs.core.windows.net/landing/github_api)
#           python_wheel_task: { package_name: oss_lakehouse, entry_point: github_fetch }
#           max_retries: 2
#           min_retry_interval_millis: 600000
#         - task_key: merge_silver
#           depends_on: [{ task_key: fetch_landing }]
#           python_wheel_task: { package_name: oss_lakehouse, entry_point: github_merge }
# ```
#
# **Landing → bronze com Auto Loader** (em vez de `spark.read` em lote, notebook 03):
#
# ```python
# (spark.readStream.format("cloudFiles")
#    .option("cloudFiles.format", "json").option("multiLine", "true")
#    .option("cloudFiles.schemaLocation", ckpt + "/schema")
#    .load(landing + "/repos")
#  .writeStream.trigger(availableNow=True).option("checkpointLocation", ckpt)
#  .toTable("oss_lakehouse_prod.bronze.github_repos_raw"))
# ```
#
# **Outras opções na plataforma**
# - **Lakeflow Connect**: conectores gerenciados (SaaS como Salesforce/Workday e CDC de bancos como SQL Server) —
#   quando existe conector pronto, ele substitui este código inteiro.
# - **Python Data Source API** (Spark 4): embrulhar a API como `spark.read.format("github")`, inclusive como fonte
#   de streaming — útil quando várias equipes consomem a mesma API.
# - **Rede**: cluster em VNet injetada sai para a internet por NAT Gateway com IP fixo — e esse IP é que conta
#   na cota sem token.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Como você ingere uma API paginada sem perder nem duplicar registros?**
# <details><summary>Resposta</summary>
# Sigo o header <code>Link</code> (de preferência cursor, não offset), ordeno por atualização crescente, gravo cada página
# bruta na landing, avanço a marca d'água só depois da landing completa e consolido com MERGE por chave estável.
# Duplicata é absorvida pelo MERGE; perda é evitada pela ordem dado → estado e pelo cursor.
# </details>
#
# **2. Offset × cursor: qual a diferença e quando o offset quebra?**
# <details><summary>Resposta</summary>
# Offset pede "a partir da posição N"; se a lista muda durante a leitura, itens mudam de posição e um pode ser pulado
# ou repetido. Cursor (keyset) pede "depois da chave X" e é estável. Offset também fica lento em páginas profundas.
# </details>
#
# **3. O que é idempotência num pipeline e como você garante?**
# <details><summary>Resposta</summary>
# Rodar 2× produz o mesmo resultado que 1×. Garanto com escrita por MERGE em chave natural (ou sobrescrita de
# partição determinística), estado gravado depois do dado e dedupe da origem. Isso torna seguro o retry do orquestrador.
# </details>
#
# **4. Full load × incremental: como decidir?**
# <details><summary>Resposta</summary>
# Incremental quando o volume é grande e há campo confiável de mudança (<code>updated_at</code>, sequência, CDC). Full quando a
# tabela é pequena, a fonte não tem marca confiável ou preciso de deletes. Na prática: incremental diário +
# reconciliação full periódica.
# </details>
#
# **5. Por que gravar a resposta bruta antes de transformar?**
# <details><summary>Resposta</summary>
# Para reprocessar sem chamar a API de novo (cota, dado que pode ter mudado ou sumido), auditar o que a fonte de fato
# respondeu e separar falha de ingestão de falha de transformação.
# </details>
#
# **6. Um 304 com ETag gasta cota no GitHub?**
# <details><summary>Resposta</summary>
# Medido aqui: sem token, gastou. A documentação isenta o 304 de requisições autenticadas. O ETag sempre economiza
# corpo e processamento; a isenção de cota depende de autenticar.
# </details>
#
# **7. A API respondeu 429 / 403 de limite secundário. O que seu código faz?**
# <details><summary>Resposta</summary>
# Lê <code>Retry-After</code> e espera exatamente isso; sem o header, backoff exponencial com jitter. Se for o limite primário
# esgotado com reset distante, levanta exceção específica e o orquestrador reagenda — não dorme segurando cluster.
# </details>
#
# **8. Como você faz backfill de 2 anos numa API com cota?**
# <details><summary>Resposta</summary>
# É o mesmo incremental com a marca reposicionada, em fatias que cabem na cota (freio de páginas por execução), com
# chave de estado própria para não atrapalhar a carga diária; token de App para cota maior; e idempotência para
# poder parar e retomar a qualquer momento.
# </details>
#
# **9. Onde fica o token da API no Databricks/Azure?**
# <details><summary>Resposta</summary>
# No Key Vault, exposto por secret scope apoiado no cofre; o código lê com <code>dbutils.secrets.get</code> e o valor sai
# mascarado em logs. Nunca em notebook, repositório ou variável de cluster em texto puro.
# </details>
#
# **10. Como incremental por <code>updated_at</code> lida com registros apagados?**
# <details><summary>Resposta</summary>
# Não lida — delete não altera <code>updated_at</code> de nada. Precisa de reconciliação full periódica (anti-join entre
# fonte e destino), de evento de delete (webhook) ou de CDC na origem.
# </details>

# %% [markdown]
# ## Resumo
#
# - Cliente de API de produção: sessão, timeout, versão fixada, token de fora, retry só no transitório (respeitando
#   `Retry-After`), falha rápida quando a cota primária acabou.
# - Landing bruta com envelope de linhagem → Silver por MERGE na chave estável (`repo_id`), com guarda de "mais novo":
#   reprocessar é seguro e não volta à API.
# - Incremental = marca d'água persistida **depois** do dado, `since` inclusivo + MERGE para absorver repetição,
#   ordem crescente para poder parar no meio (backfill em fatias).
# - Siga o header `Link`: no GitHub ele já é cursor `(updated_at, id)`; offset pula registro em lista viva.
# - ETag economiza transferência sempre; cota, só autenticado (medido aqui).

# %%
spark.stop()
