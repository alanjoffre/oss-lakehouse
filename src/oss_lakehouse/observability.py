"""Observabilidade de pipeline: registro de execução por etapa, checagens de SLA/volume,
progresso de streaming e estimativa de custo.

Os três sinais que um time de dados precisa enxergar sem abrir a Spark UI:

1. **Execução** — cada etapa (pipeline, etapa, início, fim, duração, linhas lidas/escritas,
   status, erro) vira uma linha na tabela Delta `ops/pipeline_runs`. É o "log estruturado"
   que permite responder "o que falhou ontem às 3h e quanto tempo levou" com SQL.
2. **Dado** — freshness (o dado mais novo tem quantas horas?) e volume (chegou o número de
   linhas de sempre?). Job verde com dado errado é o incidente mais caro: ninguém percebe.
3. **Custo** — DBU × preço + VM. No Databricks a fonte é `system.billing.usage` (notebook 15).

`track_step` é um context manager: o registro acontece no `finally`, então etapa que falha
também é registrada — e a exceção continua subindo (observar não pode engolir erro).
"""

from __future__ import annotations

import logging
import statistics
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any, ParamSpec, Protocol, TypeVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from oss_lakehouse.config import get_settings

log = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# ---------------------------------------------------------------------------------------------
# 1. Registro de execução por etapa
# ---------------------------------------------------------------------------------------------

RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("step", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_s", DoubleType(), True),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error", StringType(), True),
        StructField("job_group", StringType(), True),
    ]
)

RUNNING, SUCCESS, FAILED = "RUNNING", "SUCCESS", "FAILED"


def ops_path(*parts: str) -> str:
    """Caminho da área operacional (`<data_root>/ops/...`), separada das camadas de dado."""
    return "/".join([get_settings().data_root.rstrip("/"), "ops", *parts])


def new_run_id() -> str:
    """Id de uma execução do pipeline (no Databricks, use o `{{job.run_id}}` do job)."""
    return uuid.uuid4().hex


@dataclass
class StepRun:
    """Uma execução de etapa. A etapa preenche `rows_read`/`rows_written` durante o bloco.

    `run_id` identifica a execução do PIPELINE e é compartilhado pelas etapas dela (como o
    run de um job e seus task runs): a chave da tabela é (`run_id`, `step`). É o que permite
    reconstruir a sequência de etapas de uma execução — o "trace" do pipeline.
    """

    pipeline: str
    step: str
    run_id: str = field(default_factory=new_run_id)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    duration_s: float | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    status: str = RUNNING
    error: str | None = None
    job_group: str | None = None

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        # O PySpark trata datetime SEM fuso como hora local da máquina: num laptop em -03:00 o
        # instante gravado sairia 3 h errado. Datetime COM fuso é convertido certo — então
        # garantimos o fuso (sem fuso = assumimos UTC) em vez de removê-lo.
        for key in ("started_at", "finished_at"):
            if row[key] is not None and row[key].tzinfo is None:
                row[key] = row[key].replace(tzinfo=UTC)
        return row


class RunSink(Protocol):
    """Destino das execuções de etapa: qualquer objeto com `write(run)` (`MemorySink`, `DeltaRunSink`…)."""

    def write(self, run: StepRun) -> None: ...


@dataclass
class MemorySink:
    """Destino em memória — para teste e para a demonstração sem Spark."""

    runs: list[StepRun] = field(default_factory=list)

    def write(self, run: StepRun) -> None:
        self.runs.append(run)


@dataclass
class DeltaRunSink:
    """Grava cada execução como uma linha na tabela Delta `path` (append).

    Append de 1 linha por etapa gera arquivos pequenos: em produção, compactar a tabela
    (OPTIMIZE / auto compaction) ou acumular e gravar em lote no fim do job.
    """

    spark: SparkSession
    path: str = field(default_factory=lambda: ops_path("pipeline_runs"))

    def write(self, run: StepRun) -> None:
        df = self.spark.createDataFrame([run.to_row()], schema=RUNS_SCHEMA)
        df.write.format("delta").mode("append").save(self.path)

    def read(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.path)


def last_commit_metrics(spark: SparkSession, path: str) -> dict[str, str]:
    """`operationMetrics` do último commit da tabela Delta em `path` (ex.: `numOutputRows`).

    O Delta já contou as linhas ao gravar: ler a métrica do log evita pagar um `count()`
    (um scan a mais) só para preencher `rows_written`.
    """
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, path).history(1).select("operation", "operationMetrics").first()
    if row is None:
        return {}
    return {"operation": row["operation"], **dict(row["operationMetrics"] or {})}


@contextmanager
def track_step(
    pipeline: str,
    step: str,
    sink: RunSink,
    spark: SparkSession | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    run_id: str | None = None,
) -> Iterator[StepRun]:
    """Registra a execução de uma etapa — sucesso ou falha.

    Uso::

        with track_step("gh_daily", "agrega_por_tipo", sink, spark) as run:
            df = spark.read...;  run.rows_read = df.count()
            ...;                 run.rows_written = out.count()

    Se `spark` for passado, os jobs do bloco ganham o job group `run.job_group`: dá para
    achar depois os stages/tasks desta etapa na Spark UI (ver `oss_lakehouse.perf`).
    Passe o mesmo `run_id` a todas as etapas de uma execução do pipeline (`new_run_id()`);
    sem ele, cada etapa ganha um id próprio.
    """
    run = StepRun(pipeline=pipeline, step=step, started_at=clock())
    if run_id is not None:
        run.run_id = run_id
    run.job_group = f"{pipeline}.{step}.{run.run_id[:8]}"
    if spark is not None:
        spark.sparkContext.setJobGroup(run.job_group, f"{pipeline} / {step}")
    try:
        yield run
        run.status = SUCCESS
    except BaseException as exc:
        run.status = FAILED
        run.error = f"{type(exc).__name__}: {str(exc).strip().splitlines()[0][:500] if str(exc) else ''}"
        raise
    finally:
        run.finished_at = clock()
        run.duration_s = round((run.finished_at - run.started_at).total_seconds(), 3)
        if spark is not None:
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)  # type: ignore[arg-type]
            spark.sparkContext.setLocalProperty("spark.job.description", None)  # type: ignore[arg-type]
        try:
            sink.write(run)
        except Exception:  # o registro nunca pode esconder o erro original da etapa
            log.exception("falha ao registrar a execução %s/%s", pipeline, step)


def tracked(
    pipeline: str,
    step: str,
    sink: RunSink,
    spark: SparkSession | None = None,
    run_id: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator: `track_step` em volta da função.

    Se a função devolver um Mapping com `rows_read`/`rows_written`, os valores são registrados.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with track_step(pipeline, step, sink, spark, run_id=run_id) as run:
                result = func(*args, **kwargs)
                if isinstance(result, Mapping):
                    run.rows_read = result.get("rows_read", run.rows_read)
                    run.rows_written = result.get("rows_written", run.rows_written)
                return result

        return wrapper

    return decorator


# ---------------------------------------------------------------------------------------------
# 2. Checagens de dado: freshness e volume
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    """Resultado de uma checagem (freshness, volume…). `str()` dá a linha de alerta pronta para notificar."""

    name: str
    ok: bool
    observed: str
    expected: str

    def __str__(self) -> str:
        flag = "OK    " if self.ok else "ALERTA"
        return f"[{flag}] {self.name}: observado {self.observed} | esperado {self.expected}"


def check_freshness(
    last_event: datetime, now: datetime, sla: timedelta, name: str = "freshness"
) -> CheckResult:
    """O dado mais novo da tabela tem no máximo `sla` de idade?"""
    age = now - last_event
    hours = age.total_seconds() / 3600
    return CheckResult(
        name=name,
        ok=age <= sla,
        observed=f"dado mais novo com {hours:,.1f} h",
        expected=f"<= {sla.total_seconds() / 3600:,.1f} h",
    )


def check_volume(
    current: int, history: Sequence[int], tolerance: float = 0.5, name: str = "volume"
) -> CheckResult:
    """O volume atual está a até ±`tolerance` da mediana do histórico?

    Mediana (e não média) porque um dia anômalo no histórico não deve mover a régua.
    """
    if not history:
        return CheckResult(name, True, f"{current:,} linhas", "sem histórico (primeira carga)")
    baseline = statistics.median(history)
    low, high = baseline * (1 - tolerance), baseline * (1 + tolerance)
    return CheckResult(
        name=name,
        ok=low <= current <= high,
        observed=f"{current:,} linhas",
        expected=f"{low:,.0f} a {high:,.0f} (mediana {baseline:,.0f} ±{tolerance:.0%})",
    )


def raise_alerts(results: Sequence[CheckResult], notify: Callable[[str], None] = print) -> list[CheckResult]:
    """Envia cada checagem que falhou ao `notify` (print, Slack, e-mail…) e devolve as falhas."""
    failed = [r for r in results if not r.ok]
    for r in failed:
        notify(f"🚨 {r}")
    return failed


# ---------------------------------------------------------------------------------------------
# 3. Streaming: progresso de cada micro-lote
# ---------------------------------------------------------------------------------------------


class ProgressCollector(StreamingQueryListener):
    """Listener que guarda o progresso de cada micro-lote (o que a aba Structured Streaming mostra).

    Em produção, `onQueryProgress` enviaria a métrica para o monitoramento (Azure Monitor,
    Datadog, tabela Delta) e alertaria se `processedRowsPerSecond` < `inputRowsPerSecond`
    por muito tempo (o stream está ficando para trás).
    """

    def __init__(self) -> None:
        self.progress: list[dict[str, Any]] = []
        self.terminated: list[dict[str, Any]] = []

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802 (API do Spark)
        log.info("stream iniciado: %s", event.id)

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        p = event.progress
        self.progress.append(
            {
                "query": p.name,
                "batch_id": p.batchId,
                "num_input_rows": p.numInputRows,
                "input_rows_per_s": round(p.inputRowsPerSecond, 1),
                "processed_rows_per_s": round(p.processedRowsPerSecond, 1),
                "trigger_ms": dict(p.durationMs).get("triggerExecution"),
                "timestamp": p.timestamp,
            }
        )

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        self.terminated.append({"id": str(event.id), "exception": event.exception})


# ---------------------------------------------------------------------------------------------
# 4. Custo (FinOps)
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CostEstimate:
    """Saída de `estimate_cost`: DBUs consumidos e custo de DBU e de VM, na moeda dos preços informados."""

    dbus: float
    dbu_cost: float
    vm_cost: float

    @property
    def total(self) -> float:
        return self.dbu_cost + self.vm_cost


def estimate_cost(
    hours: float,
    nodes: int,
    dbu_per_node_hour: float,
    dbu_price: float,
    vm_price_per_node_hour: float = 0.0,
) -> CostEstimate:
    """Custo de computação no Databricks clássico: DBU × preço do DBU + VM do provedor.

    `nodes` inclui o driver. Em serverless a VM já está no preço do DBU (`vm_price=0`).
    Os preços variam por região, plano, tipo de compute e acordo comercial: nunca chumbar —
    ler de `system.billing.list_prices` ou da tabela de preços atual.
    """
    if hours < 0 or nodes < 1:
        raise ValueError("horas >= 0 e nodes >= 1")
    dbus = hours * nodes * dbu_per_node_hour
    return CostEstimate(
        dbus=dbus,
        dbu_cost=dbus * dbu_price,
        vm_cost=hours * nodes * vm_price_per_node_hour,
    )
