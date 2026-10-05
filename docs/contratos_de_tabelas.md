# Contratos das tabelas

Um **contrato de dados** (*data contract*) é o acordo entre quem produz e quem consome uma tabela:
nome, colunas, tipos, chave, granularidade, frequência e dono. Mudar o contrato sem avisar é quebrar o consumidor.

Raiz: `get_settings().data_root` (local `data/`; Databricks `abfss://lake@<conta>.dfs.core.windows.net`).

## Fontes (landing)

| Fonte | Caminho | Formato | Observação |
|---|---|---|---|
| GH Archive | `landing/gharchive/` | `YYYY-MM-DD-H.json.gz` (1 por hora, UTC) | `make data` deixa 3 horas (12–14h de 2026-10-01); o dia inteiro fica em `data/raw_cache/gharchive/` para simular chegada de arquivo novo e para o notebook de performance |
| API REST do GitHub | `landing/github_api/` | JSON por página | enriquecimento de repositórios (notebook 04) |
| Wikimedia EventStreams | `landing/wikimedia/` | JSON lines por micro-lote | streaming (notebook 06); amostra gravada em `tests/fixtures/wikimedia/` para rodar offline |

## Bronze

### `bronze/gh_events` — dono: `oss_lakehouse.bronze` (notebook 03)

Grão: 1 linha por evento recebido (pode haver duplicata entre arquivos — a Silver resolve). Particionada por `event_date`.

| Coluna | Tipo | Descrição |
|---|---|---|
| id | string | id do evento no GitHub |
| type | string | PushEvent, PullRequestEvent, IssuesEvent, WatchEvent… (16 tipos) |
| actor | struct<id, login, display_login, url, avatar_url> | quem fez |
| repo | struct<id, name, url> | onde (`repo.id` é estável; `repo.name` muda em renomeação) |
| org | struct<id, login> | organização (nula para repo pessoal) |
| payload | string (JSON bruto) | formato varia por `type` |
| public | boolean | |
| created_at | string (ISO-8601 UTC) | |
| _source_file | string | arquivo de origem (linhagem) |
| _ingested_at | timestamp | quando entrou |
| event_date | date | partição |

Escrita **somente** por `ingest_gharchive_bronze` (append incremental, checkpoint em `_checkpoints/bronze_gh_events`).
Qualquer outro notebook só **lê**.

## Silver / Gold

Donas: notebooks 05 e 07 (`oss_lakehouse.silver`, `oss_lakehouse.scd2`, `oss_lakehouse.gold`). Esquemas documentados
nesses módulos e resumidos aqui quando estáveis.

## Fatos sobre os dados (medidos em 2026-10-01 12h UTC)

- ~92 mil eventos/hora; PushEvent ≈ 77% dos eventos.
- 53 mil repositórios distintos na hora.
- **Skew natural:** `github-actions[bot]` ≈ 11% dos eventos; bots (`[bot]`) somados ≈ 14%.

## Silver / Gold — esquemas (notebooks 05, 07 e 08)

Todas as tabelas abaixo são Delta, gravadas por caminho (`get_settings().path(camada, tabela)`).
Quem escreve é só o módulo dono; os demais notebooks apenas leem.

### `silver/gh_events` — dono: `oss_lakehouse.silver` (notebook 05)

Grão: **1 linha por evento** (`event_id` único). Particionada por `event_date`; deletion vectors ligados.
Carga incremental e idempotente: stream da bronze + `foreachBatch` + MERGE por `event_id` (+ `event_date`),
com UPDATE só quando `_content_hash` muda. Checkpoint em `_checkpoints/silver_gh_events`.
Contrato versionado (fonte da verdade de tipos e nulos): `contracts/silver_gh_events.yaml`, validado no notebook 08.

| Coluna | Tipo | Descrição |
|---|---|---|
| event_id | bigint | `id` da bronze convertido — chave |
| event_type | string | `type` (PushEvent, PullRequestEvent…) |
| created_at | timestamp (UTC) | instante do evento |
| event_date, event_hour | date, int | derivadas de `created_at` (partição; hora 0–23) |
| actor_id, actor_login, is_bot | bigint, string, boolean | `actor` achatado; `is_bot` = login termina em `[bot]` |
| repo_id, repo_name, repo_owner | bigint, string, string | `repo` achatado; dono = parte antes da `/` |
| org_id, org_login | bigint, string | nulos em repositório pessoal |
| action, ref, ref_type, push_id, head_sha | string, string, string, bigint, string | extraídos do `payload` |
| pr_number, pr_id, pr_base_ref, pr_head_ref | int, bigint, string, string | eventos de PR e de review |
| issue_number, issue_title, issue_state | int, string, string | `issue_title` é texto livre (pode conter dado pessoal) |
| review_state, release_tag | string, string | review / release |
| is_public | boolean | `public` |
| _source_file, _ingested_at | string, timestamp | linhagem herdada da bronze |
| _content_hash | string | sha256 do evento de origem — decide se o MERGE reescreve |
| _processed_at | timestamp | quando a Silver gravou a versão |

### `silver/dim_repo_scd2` — dono: `oss_lakehouse.scd2` (notebook 05)

Grão: **1 linha por versão de repositório** (SCD tipo 2). Chave natural `repo_id`; atributos rastreados
`repo_name`, `repo_owner`. Sem partição. Intervalo de vigência `[valid_from, valid_to)` — `valid_to` exclusivo.

| Coluna | Tipo | Descrição |
|---|---|---|
| sk | bigint | surrogate key determinística = `xxhash64(repo_id, valid_from)` |
| repo_id | bigint | chave natural (estável) |
| repo_name, repo_owner | string | atributos versionados |
| valid_from | timestamp | início da vigência (1ª observação daquele valor) |
| valid_to | timestamp | fim da vigência; nulo = versão vigente |
| is_current | boolean | `valid_to IS NULL` |
| _row_hash | string | sha256 dos atributos rastreados |
| _updated_at | timestamp | última escrita da linha |

### `gold/*` — dono: `oss_lakehouse.gold` (notebook 07)

Star schema, sem partição; gravadas com `CREATE OR REPLACE TABLE … CLUSTER BY` (Liquid Clustering) onde indicado.

| Tabela | Tipo | Grão | Chave | Clustering |
|---|---|---|---|---|
| `gold/dim_date` | dimensão gerada | 1 dia | `date_key` (int `yyyymmdd`) | — |
| `gold/dim_actor` | dimensão SCD1 | 1 `actor_id` | `actor_sk` = `xxhash64(actor_id)` | — |
| `gold/dim_repo` | dimensão SCD2 | 1 versão de repositório | `repo_sk` (= `sk` da SCD2); `-1` = desconhecido | `repo_id` |
| `gold/fct_events` | fato transacional | 1 evento | `event_id` | `date_key, repo_id` |
| `gold/fct_repo_activity_daily` | fato agregado | repositório × dia | `date_key, repo_id` | `date_key, repo_id` |

Colunas:

- **`dim_date`**: `date_key` int, `date` date, `year`, `quarter`, `month` int, `month_name` string, `day` int,
  `day_of_week` int (1 = segunda … 7 = domingo), `day_name` string, `is_weekend` boolean.
- **`dim_actor`**: `actor_sk` bigint, `actor_id` bigint, `actor_login` string (o mais recente), `is_bot` boolean,
  `first_seen_at`, `last_seen_at` timestamp.
- **`dim_repo`**: `repo_sk` bigint, `repo_id` bigint, `repo_name`, `repo_owner` string, `valid_from`, `valid_to`
  timestamp, `is_current` boolean.
- **`fct_events`**: `event_id` bigint, `date_key` int (FK `dim_date`), `event_hour` int, `created_at` timestamp,
  `event_type`, `action` string (dimensões degeneradas), `actor_sk` bigint (FK `dim_actor`), `repo_sk` bigint
  (FK `dim_repo`, versão vigente **no instante do evento**), `repo_id` bigint (chave durável), `pr_number`,
  `issue_number` int, `review_state` string.
- **`fct_repo_activity_daily`**: `date_key` int, `repo_id` bigint, e as medidas (int) `events`, `pushes`,
  `prs_opened`, `prs_merged`, `prs_closed_unmerged`, `issues_opened`, `issues_closed`, `stars`, `forks`,
  `releases`, `bot_events` — todas aditivas — e `distinct_actors`, **não aditiva** (não somar entre repositórios nem dias).

### `quarantine/silver_gh_events` — dono: `oss_lakehouse.quality` (notebook 08)

Linhas reprovadas em alguma expectation de ação `drop`. Mesmas colunas de `silver/gh_events` mais:

| Coluna | Tipo | Descrição |
|---|---|---|
| _dq_failed_rules | array<string> | nomes das regras que a linha violou (inclusive as `warn`) |
| _dq_checked_at | timestamp | quando a checagem rodou |

Sobrescrita a cada execução do notebook 08 (demonstração); em produção seria append com retenção e dono.
