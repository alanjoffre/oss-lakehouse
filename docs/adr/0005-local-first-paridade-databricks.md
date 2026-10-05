# ADR 0005 — Desenvolvimento local-first com paridade Databricks

- **Status:** Aceito
- **Data:** 2026-10-05

## Contexto

O repositório precisa rodar inteiro sem custo e sem conta em nuvem (estudo, demonstração ao vivo, CI), e ao mesmo
tempo provar que o mesmo código roda no Azure Databricks. Ciclos de teste que dependem de subir um cluster na nuvem
custam minutos e dinheiro por iteração.

## Decisão

- **Local-first:** Spark 4.2 + `delta-spark` 4.4 no laptop (Java 17), dados em `data/`, emulador Azurite para o
  Blob/ADLS (notebook 14). Testes com Spark local (`pytest`, `chispa`).
- **Paridade por configuração, não por código:** o que muda entre ambientes é só a raiz dos dados
  (`OSSLH_DATA_ROOT`: `data/` × `abfss://…`) e a sessão Spark (`get_spark()` devolve a sessão ativa no Databricks).
  A lógica fica no pacote `oss_lakehouse`, empacotado como wheel e executado pelo job do bundle.
- **O que não existe local é marcado ☁️** nos notebooks e mostrado como código, sem simular.

## Alternativas consideradas

- **Desenvolver direto no Databricks** (notebooks na UI ou Git folders): ciclo curto para exploração, mas teste
  unitário, revisão de código e CI ficam fracos; e exige workspace pago ou os limites da Free Edition.
- **Databricks Connect** (Spark Connect apontando para um cluster/serverless remoto): o IDE local executa no
  Databricks. Ótimo para paridade, mas precisa de workspace e rede; usamos como opção, não como base.
- **Containers com a imagem do runtime** (`databricksruntime/*`): imagens antigas e pesadas; não refletem
  serverless nem Photon.

## Consequências

- (+) `make data && make test` roda offline e de graça; CI usa o mesmo caminho.
- (−) **A paridade é parcial** — e isso fica explícito na tabela do notebook 01: Auto Loader (`cloudFiles`), Photon,
  Unity Catalog, Liquid Clustering automático, Predictive Optimization, `dbutils`, system tables e serverless não
  existem localmente. Local usamos o equivalente open source (file source do Structured Streaming, caminho em vez
  de catálogo, `OPTIMIZE`/`VACUUM` explícitos).
- (−) Versões divergem: Spark local 4.2 ≈ DBR 19; o LTS de produção pode estar uma versão atrás. Testes de
  integração no Databricks (alvo `dev` do bundle) fecham essa lacuna antes de prod.
