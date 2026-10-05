# Registros de decisão de arquitetura (ADR)

Um **ADR** (*Architecture Decision Record*) é um texto curto que registra **uma** decisão de arquitetura:
o contexto que forçou a decisão, o que foi decidido, as alternativas descartadas e as consequências
(boas e ruins). Serve para que, daqui a um ano, ninguém precise adivinhar *por que* o sistema é assim —
nem refazer uma discussão já encerrada sem um fato novo.

Regras deste repositório:

- Um arquivo por decisão, numerado, imutável depois de aceito. Mudou de ideia? Novo ADR que **substitui** o antigo
  (e o antigo ganha o status "Substituído por NNNN").
- Formato: **Contexto → Decisão → Alternativas consideradas → Consequências**.
- Status: Proposto · Aceito · Substituído · Descontinuado.

| # | Decisão | Status | Notebook |
|---|---|---|---|
| [0001](0001-lakehouse-medallion.md) | Lakehouse com arquitetura Medallion (bronze/silver/gold) | Aceito | 00, 05, 07 |
| [0002](0002-delta-lake.md) | Delta Lake como formato de tabela (× Iceberg, Hudi) | Aceito | 10 |
| [0003](0003-payload-bruto-na-bronze.md) | `payload` como JSON bruto (string) na bronze | Aceito | 03, 05 |
| [0004](0004-orquestracao.md) | Orquestração com Lakeflow Jobs (× Azure Data Factory, Airflow) | Aceito | 13, 15 |
| [0005](0005-local-first-paridade-databricks.md) | Desenvolvimento local-first com paridade Databricks | Aceito | 01, 14 |
| [0006](0006-ia-com-avaliacao-e-humano.md) | IA no pipeline só com avaliação e humano no circuito | Aceito | 12 |
