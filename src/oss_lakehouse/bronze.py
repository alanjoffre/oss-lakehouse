"""Bronze: cópia fiel da fonte, incremental, com metadados de linhagem.

Regras da camada (Medallion):
- não interpretar o dado: o `payload` fica como STRING JSON bruta (o formato muda por tipo
  de evento; quem interpreta é a Silver). No Databricks a alternativa moderna é VARIANT;
- só o envelope estável ganha schema explícito — schema inferido em produção é armadilha;
- toda linha carrega de onde veio e quando entrou (`_source_file`, `_ingested_at`);
- ingestão incremental com checkpoint: cada arquivo é processado exatamente uma vez.

Local usamos o file source do Structured Streaming com `trigger(availableNow=True)`.
No Databricks a troca é uma linha: `format("cloudFiles")` (Auto Loader) — ver notebook 03.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from oss_lakehouse.config import get_settings

# Envelope estável do evento. `payload` é lido como string: o JSON source do Spark
# devolve o objeto bruto quando o campo é declarado StringType.
GH_EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField(
            "actor",
            StructType(
                [
                    StructField("id", LongType()),
                    StructField("login", StringType()),
                    StructField("display_login", StringType()),
                    StructField("url", StringType()),
                    StructField("avatar_url", StringType()),
                ]
            ),
        ),
        StructField(
            "repo",
            StructType(
                [
                    StructField("id", LongType()),
                    StructField("name", StringType()),
                    StructField("url", StringType()),
                ]
            ),
        ),
        StructField(
            "org",
            StructType([StructField("id", LongType()), StructField("login", StringType())]),
        ),
        StructField("payload", StringType()),
        StructField("public", BooleanType()),
        StructField("created_at", StringType()),
    ]
)

BRONZE_TABLE = "gh_events"


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Colunas de linhagem: arquivo de origem, momento da ingestão e data do evento (partição)."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("created_at")))
    )


def read_gharchive_stream(spark: SparkSession, landing_dir: str, max_files_per_trigger: int = 4) -> DataFrame:
    return (
        spark.readStream.schema(GH_EVENT_SCHEMA)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        # Linha que não casa com o schema não derruba o job: vai para _corrupt_record.
        .option("mode", "PERMISSIVE")
        .json(landing_dir)
        .select("*", "_metadata")
    )


def ingest_gharchive_bronze(
    spark: SparkSession,
    landing_dir: str | None = None,
    target_path: str | None = None,
    checkpoint_path: str | None = None,
) -> StreamingQuery:
    """Processa os arquivos novos da landing e para (availableNow). Rodar 2x não duplica nada."""
    s = get_settings()
    landing_dir = landing_dir or s.path("landing", "gharchive")
    target_path = target_path or s.path("bronze", BRONZE_TABLE)
    checkpoint_path = checkpoint_path or s.checkpoint("bronze_gh_events")

    df = add_ingestion_metadata(read_gharchive_stream(spark, landing_dir)).drop("_metadata")
    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .start(target_path)
    )
    query.awaitTermination()
    return query
