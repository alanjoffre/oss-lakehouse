"""Soluções reaproveitáveis dos exercícios clássicos de entrevista (notebook 16).

Python puro: merge de intervalos, contagem/top-K sem pandas, top-K de arquivo maior que a
memória (agregação externa por hash). PySpark: deduplicar mantendo o mais recente,
sessionização e *gaps and islands* (sequências de dias consecutivos).
"""

from __future__ import annotations

import heapq
import zlib
from collections import Counter
from collections.abc import Callable, Hashable, Iterable
from contextlib import ExitStack
from pathlib import Path
from typing import Protocol

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- Python puro


class SupportsLessThan(Protocol):
    """Qualquer tipo comparável com `<` (int, str, datetime…): é só o que `merge_intervals` exige."""

    def __lt__(self, other: object, /) -> bool: ...


def merge_intervals[T: SupportsLessThan](intervals: Iterable[tuple[T, T]]) -> list[tuple[T, T]]:
    """Une intervalos fechados que se sobrepõem ou se tocam. O(n log n) pela ordenação.

    [(1,3),(2,6),(8,10),(10,12)] -> [(1,6),(8,12)]
    """
    merged: list[tuple[T, T]] = []
    for start, end in sorted(intervals):
        if merged and not merged[-1][1] < start:  # start <= fim do último: sobrepõe ou encosta
            last_start, last_end = merged[-1]
            merged[-1] = (last_start, end if last_end < end else last_end)
        else:
            merged.append((start, end))
    return merged


def top_k[K: Hashable](items: Iterable[K], k: int) -> list[tuple[K, int]]:
    """Conta ocorrências e devolve as k maiores (empate: ordem alfabética/natural da chave).

    Counter é um dict: memória O(chaves distintas), não O(linhas). `heapq.nsmallest` com a
    chave (-contagem, item) evita ordenar tudo: O(n log k).
    """
    counts = Counter(items)
    return heapq.nsmallest(k, counts.items(), key=lambda kv: (-kv[1], kv[0]))


def stable_bucket(key: str, n: int) -> int:
    """Bucket determinístico. `hash()` do Python NÃO serve: muda a cada processo (PYTHONHASHSEED)."""
    return zlib.crc32(key.encode()) % n


def external_top_k(
    keys: Iterable[str],
    k: int,
    workdir: str | Path,
    n_partitions: int = 16,
    on_partition: Callable[[int, int], None] | None = None,
) -> list[tuple[str, int]]:
    """Top-K de um fluxo cujas chaves distintas NÃO cabem na memória (agregação externa por hash).

    Passo 1: espalha cada chave num de N arquivos por hash estável — a mesma chave sempre cai
    no mesmo arquivo. Passo 2: conta um arquivo por vez (cabe na memória) e mantém só um heap
    de tamanho k com os melhores globais. Memória ~ maior partição + k. É o que o Spark faz
    num `groupBy` (shuffle por hash + agregação por partição) — aqui, à mão.
    `on_partition(i, distintas)` é um gancho de observabilidade (quantas chaves cada partição teve).
    """
    work = Path(workdir)
    work.mkdir(parents=True, exist_ok=True)
    paths = [work / f"part-{i:03d}.txt" for i in range(n_partitions)]
    with ExitStack() as stack:  # N arquivos abertos, todos fechados na saída, mesmo com erro
        files = [stack.enter_context(p.open("w", encoding="utf-8")) for p in paths]
        for key in keys:
            files[stable_bucket(key, n_partitions)].write(key + "\n")

    best: list[tuple[str, int]] = []  # só k candidatos sobrevivem entre partições
    for i, p in enumerate(paths):
        with p.open(encoding="utf-8") as f:
            counts = Counter(line.rstrip("\n") for line in f)
        if on_partition:
            on_partition(i, len(counts))
        best = heapq.nsmallest(k, [*best, *counts.items()], key=lambda kv: (-kv[1], kv[0]))
        p.unlink()
    return best


# --------------------------------------------------------------------------- PySpark


def _cols(names: str | list[str]) -> list[str]:
    return [names] if isinstance(names, str) else list(names)


def dedup_latest(df: DataFrame, keys: str | list[str], order_by: list[Column]) -> DataFrame:
    """Mantém 1 linha por chave: a primeira segundo `order_by` (ex.: [F.col('ts').desc()]).

    ROW_NUMBER (e não RANK): com empate em `ts`, RANK devolveria 2 linhas para a mesma chave.
    Inclua um desempate determinístico em `order_by` (ex.: id), senão o resultado varia entre execuções.
    """
    w = Window.partitionBy(*_cols(keys)).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def sessionize(df: DataFrame, user: str, ts: str, gap_minutes: int = 30) -> DataFrame:
    """Acrescenta `session_id` (user + nº da sessão): nova sessão quando o intervalo > gap.

    Padrão: LAG -> flag de "começou sessão" -> soma acumulada da flag = número da sessão.
    """
    w = Window.partitionBy(user).orderBy(ts)
    prev = F.lag(ts).over(w)
    gap = F.col(ts).cast("long") - prev.cast("long")
    new_session = F.when(prev.isNull() | (gap > gap_minutes * 60), 1).otherwise(0)
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        df.withColumn("_new", new_session)
        .withColumn("session_n", F.sum("_new").over(w_cum))
        .withColumn("session_id", F.concat_ws("#", F.col(user).cast("string"), F.col("session_n")))
        .drop("_new")
    )


def activity_streaks(df: DataFrame, user: str, day: str) -> DataFrame:
    """Gaps and islands: sequências de dias consecutivos por usuário -> (user, start, end, days).

    Truque: em dias consecutivos, `day - row_number` é constante; esse valor é a "ilha".
    `dropDuplicates` antes: 2 eventos no mesmo dia quebrariam a conta do row_number.
    """
    d = df.select(user, F.col(day).cast("date").alias("_d")).dropDuplicates()
    w = Window.partitionBy(user).orderBy("_d")
    island = F.date_sub(F.col("_d"), F.row_number().over(w))
    return (
        d.withColumn("_island", island)
        .groupBy(user, "_island")
        .agg(F.min("_d").alias("start"), F.max("_d").alias("end"), F.count("*").alias("days"))
        .drop("_island")
    )
