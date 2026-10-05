# ADR 0001 — Lakehouse com arquitetura Medallion

- **Status:** Aceito
- **Data:** 2026-10-05

## Contexto

Precisamos guardar e servir eventos públicos do GitHub (~92 mil eventos por hora, JSON semiestruturado, formato
que muda por tipo de evento), enriquecimento pela API do GitHub e um fluxo contínuo da Wikimedia. Os consumidores
são variados: SQL/BI para métricas, ciência de dados para exploração e um caso de IA aplicada (notebook 12).

Requisitos que pesam:

- reprocessar a história quando uma regra muda (sem baixar tudo de novo);
- transações ACID (*atomicidade, consistência, isolamento, durabilidade*) em cima de armazenamento de objetos barato;
- uma só cópia do dado servindo SQL, Python e ML — sem exportar para outro sistema a cada uso;
- governança central (Unity Catalog) e custo proporcional ao uso.

## Decisão

Adotar um **lakehouse** — tabelas transacionais (Delta Lake, ADR 0002) sobre ADLS Gen2 — organizado em **camadas
Medallion**:

| Camada | O que entra | Regra |
|---|---|---|
| **Bronze** | cópia fiel da fonte, *append-only*, com linhagem (`_source_file`, `_ingested_at`) | não interpretar; reprocessável (ADR 0003) |
| **Silver** | dado limpo, tipado, deduplicado, com chaves e histórico (MERGE, SCD2) | uma verdade por entidade; contrato estável |
| **Gold** | modelo de consumo: fatos/dimensões, agregados por caso de uso | pronto para BI/IA; nomes de negócio |

Dado reprovado em qualidade vai para `quarantine/`, não some.

## Alternativas consideradas

- **Data warehouse puro** (Synapse dedicated pool, Snowflake, BigQuery): ótimo para SQL/BI, mas JSON bruto,
  reprocessamento e ML ficam caros ou exigem cópia para um lake ao lado — duas cópias, duas governanças.
- **Data lake sem formato de tabela** (Parquet solto em pastas): barato, mas sem ACID, sem MERGE, sem time travel;
  leitura concorrente com escrita vê arquivo pela metade. É o "pântano de dados" (*data swamp*).
- **Camadas diferentes do Medallion** (ex.: *raw → staging → marts* do dbt, ou *Data Vault* na silver): equivalentes
  em espírito. Ficamos com Medallion por ser a nomenclatura da plataforma alvo (Databricks) e do mercado.

## Consequências

- (+) Reprocessar = reler a bronze; a fonte externa não é tocada de novo.
- (+) Cada camada tem um dono e um contrato (`docs/contratos_de_tabelas.md`).
- (−) Mais cópias do dado (bronze+silver+gold) → custo de storage e de compute para mantê-las. Mitigação: retenção
  na bronze e VACUUM (notebook 10).
- (−) O Medallion é critério de **qualidade**, não de modelagem: não diz como modelar a gold. Isso vem do Kimball
  (notebook 07). Camadas demais ("silver2", "gold_final") são sinal de modelagem ruim, não de rigor.
- (−) Latência: cada salto de camada soma tempo. Onde segundos importam, usa-se streaming fim a fim (notebook 06).
