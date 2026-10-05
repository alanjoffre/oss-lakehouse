# ADR 0004 — Orquestração com Lakeflow Jobs

- **Status:** Aceito
- **Data:** 2026-10-05

## Contexto

O pipeline tem dependências (bronze → silver → gold → quality), roda de hora em hora, precisa de retry, timeout,
alerta, histórico de execução e deploy versionado (CI/CD). Todo o processamento roda no Databricks; não há,
por ora, sistemas fora dele a coordenar (bancos on-premises, SaaS, APIs de terceiros com fluxos complexos).

## Decisão

Orquestrar com **Lakeflow Jobs** (o nome atual dos *Databricks Workflows/Jobs*), definidos como código em
**Declarative Automation Bundles** (ex-*Databricks Asset Bundles*; `databricks.yml` + `resources/jobs.yml`),
com deploy pelo GitHub Actions (notebook 13).

## Alternativas consideradas

| | Lakeflow Jobs | Azure Data Factory (ADF) | Apache Airflow (ou Azure/Astronomer gerenciado) |
|---|---|---|---|
| Onde brilha | tudo dentro do Databricks; zero infraestrutura extra | cópia de dados de dezenas de fontes (conectores, *integration runtime* on-premises) | orquestração de muitos sistemas heterogêneos, DAGs dinâmicos em Python |
| Custo extra | nenhum além do compute | por atividade/execução + DIU de cópia | servidor/serviço do Airflow sempre ligado |
| Como código | Bundles (YAML) | ARM/Bicep/JSON + Git integration do ADF Studio | Python (DAGs) |
| Observabilidade | UI de jobs, system tables (`system.lakeflow.*`) | monitor do ADF + Azure Monitor | UI do Airflow + o que você montar |
| Gatilhos | agenda, chegada de arquivo, atualização de tabela, contínuo | agenda, evento de storage, tumbling window | agenda, sensores, datasets |

- **ADF** continua útil como **camada de ingestão** quando a fonte está atrás de firewall corporativo
  (self-hosted integration runtime) — e pode disparar um Lakeflow Job. Não como orquestrador principal aqui.
- **Airflow** se justifica quando o Databricks é só um dos muitos sistemas a coordenar; traz custo de operação.

## Consequências

- (+) Uma ferramenta a menos para operar; histórico e custo do job ficam nas system tables (notebook 15).
- (+) Job, cluster, permissões e agenda versionados junto com o código; `dev` isolado por pessoa.
- (−) Acoplamento à plataforma: migrar de Databricks exige reescrever a orquestração (o código do pacote é portável;
  o YAML do bundle, não).
- (−) Orquestração entre sistemas externos fica limitada; se isso crescer, este ADR deve ser revisto.
- Local, a orquestração é o `Makefile` (`make data bronze …`) — mesma ordem de dependências, sem agenda.
