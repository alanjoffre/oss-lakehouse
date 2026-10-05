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
