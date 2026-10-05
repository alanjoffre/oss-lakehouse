# Trilha de notebooks

| # | Notebook (arquivo em `notebooks/_src/`) | Tema |
|---|---|---|
| 00 | `00_mapa_da_vaga_e_arquitetura.py` | Requisitos da vaga → onde cada um aparece; arquitetura Lakehouse/Medallion na Azure; ADRs |
| 01 | `01_ambiente_local_databricks_azure.py` | Spark+Delta local, Databricks Free Edition, Azure; o que muda entre eles |
| 02 | `02_python_avancado.py` | Python para engenharia de dados: typing, generators, decorators, context managers, dataclasses/pydantic, pytest |
| 03 | `03_ingestao_arquivos_auto_loader.py` | Ingestão incremental de arquivos (GH Archive): Auto Loader, checkpoint, schema evolution, rescued data, backfill |
| 04 | `04_ingestao_api_incremental.py` | API REST do GitHub: paginação, rate limit, ETag, retry, marca d'água |
| 05 | `05_bronze_silver_merge_scd2.py` | Limpeza, tipagem, deduplicação, MERGE, SCD tipo 1 e 2, dado atrasado |
| 06 | `06_streaming_structured_streaming.py` | Wikimedia EventStreams: Structured Streaming, watermark, janelas, foreachBatch, exactly-once |
| 07 | `07_gold_modelagem_dimensional.py` | Kimball: fatos, dimensões, star schema, agregados, Liquid Clustering |
| 08 | `08_qualidade_e_contratos.py` | Expectations, quarentena, data contracts, Spark Declarative Pipelines / Lakeflow |
| 09 | `09_performance_spark.py` | Plano físico, shuffle, skew (salting/AQE), broadcast, small files, particionamento × Z-order × Liquid |
| 10 | `10_delta_lake_por_dentro.py` | Transaction log, time travel, RESTORE, VACUUM, CDF, concorrência, deletion vectors |
| 11 | `11_governanca_unity_catalog_lgpd.py` | Unity Catalog, grants, row filter, column mask, lineage, LGPD |
| 12 | `12_ia_aplicada_engenharia_de_dados.py` | LLM no pipeline: PII, classificação, regras de qualidade, documentação — com avaliação |
| 13 | `13_cicd_git_asset_bundles.py` | Git (branching, PR, versionamento), Databricks Asset Bundles, GitHub Actions, ambientes |
| 14 | `14_azure_terraform_azurite.py` | ADLS Gen2, Access Connector, Key Vault, Azure Databricks via Terraform; Azurite local |
| 15 | `15_observabilidade_e_custo.py` | Métricas de pipeline, system tables, alertas, FinOps |
| 16 | `16_live_coding_sql_pyspark.py` | Exercícios clássicos de entrevista em SQL e PySpark |
| 17 | `17_simulado_system_design.py` | System design de pipeline + troubleshooting (job lento, OOM, duplicatas, small files) |
