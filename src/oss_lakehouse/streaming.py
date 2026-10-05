"""Structured Streaming sobre a landing da Wikimedia: schema, tempo de evento, janelas e sinks idempotentes.

Peças reaproveitáveis do notebook 06:
- `read_wiki_stream`: file source com schema explícito (stream nunca infere schema);
- `parse_wiki`: tempo de evento (`event_time`) separado do tempo de processamento (`_ingested_at`);
- `edits_per_window`: janela tumbling/sliding com watermark;
- `merge_batch`: função de `foreachBatch` que faz MERGE idempotente no Delta — reprocessar o mesmo
  micro-lote (o que acontece após uma falha) não muda o resultado;
- `progress_summary`: o que olhar no `StreamingQueryProgress`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

WIKI_SCHEMA = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("dt", StringType()),
                    StructField("domain", StringType()),
                    StructField("partition", IntegerType()),
                    StructField("offset", LongType()),
                ]
            ),
        ),
        StructField("id", LongType()),
        StructField("type", StringType()),
        StructField("namespace", IntegerType()),
        StructField("title", StringType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("timestamp", LongType()),
        StructField("wiki", StringType()),
        StructField("server_name", StringType()),
        StructField("length", StructType([StructField("old", LongType()), StructField("new", LongType())])),
        StructField("log_type", StringType()),
    ]
)


def read_wiki_stream(spark: SparkSession, path: str, max_files_per_trigger: int = 1) -> DataFrame:
    """Stream de arquivos JSONL(.gz) da landing. `maxFilesPerTrigger` controla o tamanho do micro-lote."""
    return (
        spark.readStream.schema(WIKI_SCHEMA)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        # Só arquivos prontos: o consumidor grava `.jsonl.gz.tmp` e renomeia no fim.
        .option("pathGlobFilter", "*.jsonl.gz")
        .json(path)
        .select("*", "_metadata")
    )


def parse_wiki(df: DataFrame) -> DataFrame:
    """Envelope → colunas da Silver. `event_time` vem do evento; `_ingested_at`, do relógio do cluster."""
    return df.select(
        F.col("meta.id").alias("event_id"),
        F.to_timestamp("meta.dt").alias("event_time"),
        "wiki",
        "type",
        "namespace",
        "title",
        "user",
        F.coalesce("bot", F.lit(False)).alias("is_bot"),
        (F.col("length.new") - F.col("length.old")).alias("bytes_delta"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def edits_per_window(
    events: DataFrame,
    window: str = "1 minute",
    slide: str | None = None,
    watermark: str = "1 minute",
    by: Sequence[str] = ("wiki", "is_bot"),
) -> DataFrame:
    """Contagem por janela de tempo de EVENTO. `slide=None` → tumbling; `slide` < `window` → sliding.

    O watermark diz ao Spark até quando esperar dado atrasado: janela cujo fim ficou para trás do
    watermark é finalizada (sai em `append`) e o estado dela é descartado; evento mais velho que o
    watermark é ignorado (contado em `numRowsDroppedByWatermark`).
    """
    w = F.window("event_time", window, slide) if slide else F.window("event_time", window)
    return (
        events.withWatermark("event_time", watermark)
        .groupBy(w.alias("w"), *by)
        .agg(F.count("*").alias("edits"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), *by, "edits")
    )


def _merge_condition(keys: Sequence[str]) -> str:
    return " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)


def merge_batch(
    target_path: str,
    keys: Sequence[str],
    insert_only: bool = False,
    on_batch: Callable[[int, int], None] | None = None,
) -> Callable[[DataFrame, int], None]:
    """Função para `foreachBatch`: MERGE do micro-lote no Delta em `target_path`.

    - `insert_only=True`: deduplicação (só insere chave nova) — para eventos brutos;
    - `insert_only=False`: upsert — para agregados em modo `update`, em que cada linha traz o
      valor ATUAL da janela (não um incremento). Aplicar duas vezes dá o mesmo resultado.

    Por isso é idempotente: se o job cair depois do MERGE e antes de o checkpoint registrar o
    lote, o Spark reexecuta o mesmo `batch_id` e nada duplica.
    """
    from delta.tables import DeltaTable

    def _fn(batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        # persist: o micro-lote é usado mais de uma vez (MERGE + contagem). Sem cache, cada ação
        # reexecuta o plano do lote inteiro — relê a fonte, refaz a agregação com estado e soma de
        # novo as métricas (ex.: `numRowsDroppedByWatermark` sai dobrado).
        src = batch.dropDuplicates(list(keys)).persist()
        try:
            if not DeltaTable.isDeltaTable(spark, target_path):
                src.limit(0).write.format("delta").save(target_path)
            tgt = DeltaTable.forPath(spark, target_path)
            m = tgt.alias("t").merge(src.alias("s"), _merge_condition(keys))
            if not insert_only:
                m = m.whenMatchedUpdateAll()
            m.whenNotMatchedInsertAll().execute()
            if on_batch:
                on_batch(batch_id, src.count())
        finally:
            src.unpersist()

    return _fn


def progress_summary(p: dict[str, Any]) -> dict[str, Any]:
    """As métricas do `StreamingQueryProgress` que respondem "o stream está saudável?"."""
    ops = p.get("stateOperators") or [{}]
    return {
        "batchId": p.get("batchId"),
        "numInputRows": p.get("numInputRows"),
        "inputRowsPerSecond": round(p.get("inputRowsPerSecond") or 0.0, 1),
        "processedRowsPerSecond": round(p.get("processedRowsPerSecond") or 0.0, 1),
        "triggerExecution_ms": (p.get("durationMs") or {}).get("triggerExecution"),
        "watermark": (p.get("eventTime") or {}).get("watermark"),
        "stateRows": ops[0].get("numRowsTotal"),
        "droppedByWatermark": ops[0].get("numRowsDroppedByWatermark"),
    }


def dedup_events(events: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Remove reentrega (at-least-once da fonte) por `event_id`, com estado limitado pelo watermark.

    `dropDuplicatesWithinWatermark` (Spark 3.5+) guarda cada id só enquanto o watermark não passou:
    sem watermark, o estado de dedupe cresceria para sempre.
    """
    return events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(["event_id"])
