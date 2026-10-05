"""Inferência em lote dentro do Spark: lotes determinísticos, concorrência limitada, rate limit.

O desenho que escala (e que é o mesmo do `mapInPandas` no Databricks):

    DataFrame ──► coluna `lote` determinística ──► repartition(n, lote)
        └─► por partição: agrupa por lote ─► ThreadPool(k) ─► TokenBucket ─► LLMClient (cache na frente)

- **Concorrência global** = nº de partições × k threads. É ela que precisa caber no rate limit
  do provedor (requisições/min e tokens/min) — não o tamanho do cluster.
- **Lote determinístico**: o mesmo conjunto de linhas gera o mesmo pedido → mesma chave de cache
  → reprocessar (retry de task, job reexecutado) não paga de novo. Idempotência por hash.
- O cliente é criado DENTRO da partição (`client_factory`): cliente HTTP não é serializável e
  cada executor precisa do seu.

Local usamos `rdd.mapPartitions` (sem pandas/pyarrow no ambiente); no Databricks a mesma função
de partição roda em `mapInPandas` trocando a conversão de linhas (ver notebook 12 ☁️).
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession

from oss_lakehouse.ai.client import LLMClient, LLMRequest, LLMResponse


class TokenBucket:
    """Rate limiter clássico: `taxa` fichas por segundo, rajada de até `capacidade`.

    Thread-safe; `clock` e `sleep` injetáveis para teste sem esperar.
    """

    def __init__(
        self,
        taxa: float,
        capacidade: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.taxa, self.capacidade = taxa, capacidade
        self.clock, self.sleep = clock, sleep
        self.fichas = capacidade
        self.ultimo = clock()
        self._lock = threading.Lock()

    def adquirir(self, n: float = 1.0) -> float:
        """Bloqueia até ter `n` fichas. Devolve quanto esperou (segundos)."""
        esperado = 0.0
        while True:
            with self._lock:
                agora = self.clock()
                self.fichas = min(self.capacidade, self.fichas + (agora - self.ultimo) * self.taxa)
                self.ultimo = agora
                if self.fichas >= n:
                    self.fichas -= n
                    return esperado
                falta = (n - self.fichas) / self.taxa
            self.sleep(falta)
            esperado += falta


def processar_lotes(
    linhas: Iterable[dict[str, Any]],
    col_lote: str,
    col_id: str,
    montar_request: Callable[[list[dict[str, Any]]], LLMRequest],
    interpretar: Callable[[LLMResponse, list[dict[str, Any]]], Iterable[dict[str, Any]]],
    client: LLMClient,
    max_concorrencia: int = 4,
    limiter: TokenBucket | None = None,
) -> Iterator[dict[str, Any]]:
    """Núcleo puro (sem Spark): agrupa linhas por lote e chama o LLM em paralelo, com limite."""
    grupos: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for linha in linhas:
        grupos[linha[col_lote]].append(linha)

    def um_lote(itens: list[dict[str, Any]]) -> list[dict[str, Any]]:
        itens = sorted(itens, key=lambda r: r[col_id])
        if limiter is not None:
            limiter.adquirir()
        resp = client.complete(montar_request(itens))
        return list(interpretar(resp, itens))

    with ThreadPoolExecutor(max_workers=max_concorrencia) as pool:
        for resultado in pool.map(um_lote, [grupos[k] for k in sorted(grupos)]):
            yield from resultado


def inferir_em_lote_spark(
    spark: SparkSession,
    df: DataFrame,
    col_lote: str,
    col_id: str,
    montar_request: Callable[[list[dict[str, Any]]], LLMRequest],
    interpretar: Callable[[LLMResponse, list[dict[str, Any]]], Iterable[dict[str, Any]]],
    client_factory: Callable[[], LLMClient],
    schema_saida: str,
    particoes: int = 2,
    max_concorrencia: int = 2,
    req_por_segundo: float = 1.0,
) -> DataFrame:
    """Aplica `processar_lotes` em cada partição. Rate limit por partição = req_por_segundo;
    o global é `particoes × req_por_segundo` — dimensione pelo limite do provedor."""
    colunas: Sequence[str] = df.columns

    def por_particao(it: Iterator[Row]) -> Iterator[dict[str, Any]]:
        client = client_factory()
        limiter = TokenBucket(taxa=req_por_segundo, capacidade=1)
        linhas = ({c: r[c] for c in colunas} for r in it)
        yield from processar_lotes(
            linhas, col_lote, col_id, montar_request, interpretar, client, max_concorrencia, limiter
        )

    campos = [c.strip().split()[0] for c in schema_saida.split(",")]
    rdd = df.repartition(particoes, col_lote).rdd.mapPartitions(por_particao)
    return spark.createDataFrame(rdd.map(lambda d: tuple(d[c] for c in campos)), schema_saida)
