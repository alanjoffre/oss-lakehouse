# ADR 0002 — Delta Lake como formato de tabela

- **Status:** Aceito
- **Data:** 2026-10-05

## Contexto

O lakehouse (ADR 0001) precisa de um **formato de tabela aberto** (*open table format*): uma camada de metadados
sobre arquivos Parquet que dá transações ACID, evolução de schema, MERGE, time travel e leitura consistente durante
a escrita. Os três candidatos maduros são Delta Lake, Apache Iceberg e Apache Hudi.

A plataforma alvo é Azure Databricks com Unity Catalog. Local, rodamos Spark 4.2 com `delta-spark` 4.4.

## Decisão

**Delta Lake** em todas as camadas, gerenciado pelo Unity Catalog no Databricks e por caminho (`delta.load(path)`)
localmente.

## Alternativas consideradas

| Critério | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Integração com Databricks | nativo (Photon, Liquid Clustering, Predictive Optimization, deletion vectors) | leitura/escrita via Unity Catalog (managed Iceberg, REST catalog) | suporte externo |
| Ecossistema multi-motor | bom (Spark, Trino, DuckDB, Polars, delta-rs) | o mais amplo (Snowflake, BigQuery, Athena, Trino, Flink) | menor |
| Upsert intenso / CDC | MERGE + deletion vectors | MERGE (copy-on-write ou merge-on-read) | o mais forte em *upsert* e indexação |
| Rodar local sem cluster | `delta-spark` / `delta-rs` | `pyiceberg` / Spark | Spark |

- **Iceberg** seria a escolha se o dado precisasse ser escrito por vários motores de fornecedores diferentes
  (ex.: Snowflake e Databricks escrevendo a mesma tabela). Não é o caso hoje.
- **Hudi** brilha em CDC de alta frequência com índice; o nosso volume de upsert não justifica a complexidade.

## Consequências

- (+) Recursos de performance e governança da plataforma funcionam sem adaptação (notebooks 09, 10, 11).
- (+) Delta é aberto (Linux Foundation); a saída para Iceberg existe sem reescrever dados:
  **UniForm** grava metadados Iceberg ao lado do log do Delta, e o Unity Catalog expõe a tabela a clientes Iceberg.
- (−) Versão do JAR do Delta tem de casar com a do Spark (`delta-spark` 4.4 ↔ Spark 4.2 aqui) — notebook 01.
- (−) Alguns recursos (ex.: Predictive Optimization) só existem no Databricks; no código local eles viram `OPTIMIZE`
  e `VACUUM` explícitos.
