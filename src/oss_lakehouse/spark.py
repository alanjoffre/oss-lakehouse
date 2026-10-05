"""Sessão Spark com Delta Lake — a mesma chamada funciona local e no Databricks.

No Databricks a sessão já existe (`spark`) e o Delta é o formato padrão: devolvemos a ativa.
Local, montamos a sessão com as extensões do Delta e baixamos o JAR certo via
`configure_spark_with_delta_pip` (o JAR tem de casar com a versão do Spark).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from oss_lakehouse.config import get_settings


def _on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "oss-lakehouse", **extra_conf: str) -> SparkSession:
    if _on_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    s = get_settings()
    builder = (
        SparkSession.builder.master(s.spark_master)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", s.spark_driver_memory)
        # Poucas partições de shuffle: no laptop, 200 (padrão) gera tarefas minúsculas demais.
        .config("spark.sql.shuffle.partitions", str(s.shuffle_partitions))
        # Datas sempre em UTC: o GH Archive é UTC e fuso implícito é fonte clássica de bug.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    for key, value in extra_conf.items():
        builder = builder.config(key, value)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
