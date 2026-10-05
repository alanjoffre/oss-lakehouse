"""Medição de performance: cronômetro com repetição e métricas da Spark UI pela REST API.

Por que um módulo: "ficou mais rápido" sem número não vale em entrevista nem em code review.
Aqui ficam três ferramentas que os notebooks 09 e 15 usam para PROVAR o que afirmam:

- `time_it`: roda uma ação N vezes e devolve a mediana (laptop compartilhado tem ruído;
  uma única medição mente);
- `job_group`: marca os jobs de um trecho de código com um rótulo (job group), para depois
  achar exatamente os stages/tasks daquele trecho;
- `SparkUI`: cliente da REST API da Spark UI (`<uiWebUrl>/api/v1`) — a mesma fonte de dados
  das abas Jobs/Stages/SQL. Dá a distribuição de duração das tasks (skew), spill, bytes de
  shuffle e as métricas do scan (arquivos lidos × pulados).

No Databricks a mesma REST API existe por cluster (via proxy do driver), mas o caminho
idiomático é a Spark UI do job run e, para histórico, as system tables (notebook 15).
"""

from __future__ import annotations

import io
import json
import re
import statistics
import time
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------------------------
# Cronômetro
# ---------------------------------------------------------------------------------------------


@dataclass
class Stopwatch:
    """Resultado de `stopwatch()`: `seconds` é preenchido na saída do bloco `with`."""

    seconds: float = 0.0


@contextmanager
def stopwatch(clock: Callable[[], float] = time.perf_counter) -> Iterator[Stopwatch]:
    """Cronometra um bloco: `with stopwatch() as sw: ...; sw.seconds`."""
    sw = Stopwatch()
    start = clock()
    try:
        yield sw
    finally:
        sw.seconds = clock() - start


@dataclass(frozen=True)
class Timing:
    """Tempos de N execuções da mesma ação. Reporte a mediana, não a média (robusta a outlier)."""

    label: str
    runs: tuple[float, ...]

    @property
    def median(self) -> float:
        return statistics.median(self.runs)

    @property
    def best(self) -> float:
        return min(self.runs)

    def __str__(self) -> str:
        runs = ", ".join(f"{r:.2f}" for r in self.runs)
        return f"{self.label:<38} mediana {self.median:6.2f}s   (execuções: {runs})"


def time_it(
    action: Callable[[], object],
    repeat: int = 3,
    label: str = "",
    warmup: int = 0,
    clock: Callable[[], float] = time.perf_counter,
) -> Timing:
    """Executa `action` `warmup` vezes sem medir e depois `repeat` vezes medindo.

    `action` deve disparar uma *action* do Spark (count, collect, write). Medir só a
    transformação mede nada: o Spark é preguiçoso (lazy evaluation).
    """
    if repeat < 1:
        raise ValueError("repeat precisa ser >= 1")
    for _ in range(warmup):
        action()
    runs = []
    for _ in range(repeat):
        start = clock()
        action()
        runs.append(clock() - start)
    return Timing(label or getattr(action, "__name__", "ação"), tuple(runs))


def compare(timings: Sequence[Timing]) -> str:
    """Tabela de texto com a mediana de cada medição e a razão em relação à mais rápida."""
    if not timings:
        return ""
    fastest = min(t.median for t in timings)
    lines = []
    for t in timings:
        ratio = t.median / fastest if fastest > 0 else float("nan")
        lines.append(f"{t}   {ratio:4.1f}x")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------
# Estatística de duração das tasks (skew)
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DistStats:
    """Resumo de uma distribuição (duração de tasks em ms, registros por task...)."""

    n: int
    min: float
    median: float
    p95: float
    max: float

    @property
    def max_over_median(self) -> float:
        """Indicador clássico de skew: a task mais lenta dividida pela típica.

        ~1-3x é normal; dezenas de vezes = uma task segura o stage inteiro (straggler).
        """
        return self.max / self.median if self.median > 0 else float("inf") if self.max > 0 else 1.0

    def __str__(self) -> str:
        return (
            f"n={self.n}  min={self.min:,.0f}  mediana={self.median:,.0f}  p95={self.p95:,.0f}  "
            f"max={self.max:,.0f}  max/mediana={self.max_over_median:,.1f}x"
        )


def dist_stats(values: Sequence[float]) -> DistStats:
    if not values:
        raise ValueError("lista vazia")
    ordered = sorted(values)
    idx95 = max(0, round(0.95 * (len(ordered) - 1)))
    return DistStats(
        n=len(ordered),
        min=ordered[0],
        median=statistics.median(ordered),
        p95=ordered[idx95],
        max=ordered[-1],
    )


# ---------------------------------------------------------------------------------------------
# Job group: rotular os jobs de um trecho
# ---------------------------------------------------------------------------------------------


@contextmanager
def job_group(spark: SparkSession, group: str, description: str = "") -> Iterator[str]:
    """Marca todos os jobs disparados dentro do bloco com o job group `group`.

    A Spark UI (e a REST API) mostra o `jobGroup` de cada job: é assim que achamos depois
    os stages e tasks exatamente deste trecho, sem confundir com jobs anteriores.
    """
    sc = spark.sparkContext
    sc.setJobGroup(group, description or group)
    try:
        yield group
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)  # type: ignore[arg-type]
        sc.setLocalProperty("spark.job.description", None)  # type: ignore[arg-type]


@contextmanager
def spark_conf(spark: SparkSession, conf: Mapping[str, str]) -> Iterator[None]:
    """Aplica configurações SQL de sessão dentro do bloco e restaura os valores anteriores.

    Evita o erro clássico de notebook: desligar o AQE numa célula e esquecer desligado.
    """
    previous = {k: spark.conf.get(k, None) for k in conf}
    for k, v in conf.items():
        spark.conf.set(k, v)
    try:
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                spark.conf.unset(k)
            else:
                spark.conf.set(k, v)


# ---------------------------------------------------------------------------------------------
# Cliente da REST API da Spark UI
# ---------------------------------------------------------------------------------------------

_NUMBER = re.compile(r"-?[\d,]+(?:\.\d+)?")
_UNITS = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}


def parse_metric_value(text: str) -> float | None:
    """Converte o texto de uma métrica SQL da UI em número.

    Exemplos: '12' → 12; '1,234' → 1234; '10.5 MiB' → bytes; 'total (min, med, max)\\n3.0 s (…)'
    → o primeiro número (o total). Devolve None se não houver número.
    """
    lines = text.strip().splitlines()
    # Métricas por task vêm como "total (min, med, max ...)\n<total> (<min>, <med>, <max> ...)".
    if not lines:
        return None
    line = lines[1] if lines[0].startswith("total") and len(lines) > 1 else lines[0]
    m = _NUMBER.search(line)
    if not m:
        return None
    value = float(m.group(0).replace(",", ""))
    unit = line[m.end() :].strip().split(" ")[0] if line[m.end() :].strip() else ""
    unit = unit.rstrip("(").strip()
    scale = _UNITS.get(unit) or {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}.get(unit, 1)
    return value * scale


@dataclass(frozen=True)
class StageSummary:
    stage_id: int
    name: str
    num_tasks: int
    run_time_ms: int
    input_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    spill_memory_bytes: int
    spill_disk_bytes: int

    def as_row(self) -> dict[str, Any]:
        return {
            "stage": self.stage_id,
            "tasks": self.num_tasks,
            "tempo_exec_s": round(self.run_time_ms / 1000, 2),
            "entrada_MB": round(self.input_bytes / 1e6, 1),
            "shuffle_lido_MB": round(self.shuffle_read_bytes / 1e6, 1),
            "shuffle_escrito_MB": round(self.shuffle_write_bytes / 1e6, 1),
            "spill_mem_MB": round(self.spill_memory_bytes / 1e6, 1),
            "spill_disco_MB": round(self.spill_disk_bytes / 1e6, 1),
            "nome": self.name[:45],
        }


@dataclass
class SparkUI:
    """Cliente mínimo da REST API (`/api/v1`) da Spark UI da sessão atual."""

    base_url: str
    app_id: str
    timeout: float = 10.0
    _cache: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def of(cls, spark: SparkSession) -> SparkUI:
        sc = spark.sparkContext
        url = sc.uiWebUrl
        if not url:
            raise RuntimeError("Spark UI desligada (spark.ui.enabled=false)")
        return cls(base_url=url.rstrip("/"), app_id=sc.applicationId)

    # -- HTTP -----------------------------------------------------------------------------------
    def get(self, path: str) -> Any:
        url = f"{self.base_url}/api/v1/applications/{self.app_id}/{path.lstrip('/')}"
        with urllib.request.urlopen(url, timeout=self.timeout) as resp:  # noqa: S310 (localhost)
            return json.load(resp)

    def _wait_listener(self, group: str, tries: int = 20, pause: float = 0.25) -> list[dict[str, Any]]:
        """O status store da UI é atualizado de forma assíncrona (listener bus): espera os jobs
        do grupo aparecerem como terminados antes de ler as métricas."""
        jobs: list[dict[str, Any]] = []
        for _ in range(tries):
            jobs = [j for j in self.get("jobs") if j.get("jobGroup") == group]
            if jobs and all(j["status"] in ("SUCCEEDED", "FAILED") for j in jobs):
                # Stages terminados também precisam ter sido "fechados" no store.
                time.sleep(pause)
                return jobs
            time.sleep(pause)
        return jobs

    # -- Jobs / stages / tasks ------------------------------------------------------------------
    def jobs(self, group: str) -> list[dict[str, Any]]:
        return sorted(self._wait_listener(group), key=lambda j: j["jobId"])

    def stages(self, group: str) -> list[dict[str, Any]]:
        """Stages executados (não pulados) pelos jobs do grupo, em ordem."""
        ids: set[int] = set()
        for j in self.jobs(group):
            ids.update(j["stageIds"])
        out = []
        for st in self.get("stages"):
            if st["stageId"] in ids and st["status"] in ("COMPLETE", "FAILED"):
                out.append(st)
        return sorted(out, key=lambda s: (s["stageId"], s["attemptId"]))

    def stage_summaries(self, group: str) -> list[StageSummary]:
        return [
            StageSummary(
                stage_id=s["stageId"],
                name=s.get("name", ""),
                num_tasks=s["numCompleteTasks"],
                run_time_ms=s["executorRunTime"],
                input_bytes=s["inputBytes"],
                shuffle_read_bytes=s["shuffleReadBytes"],
                shuffle_write_bytes=s["shuffleWriteBytes"],
                spill_memory_bytes=s["memoryBytesSpilled"],
                spill_disk_bytes=s["diskBytesSpilled"],
            )
            for s in self.stages(group)
        ]

    def tasks(self, stage_id: int, attempt: int = 0) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        offset, page = 0, 5000
        while True:
            chunk = self.get(f"stages/{stage_id}/{attempt}/taskList?offset={offset}&length={page}")
            out.extend(chunk)
            if len(chunk) < page:
                return [t for t in out if t.get("status") == "SUCCESS"]
            offset += page

    def task_durations(self, stage_id: int, attempt: int = 0) -> DistStats:
        """Distribuição da duração (ms) das tasks de um stage."""
        return dist_stats([t["duration"] for t in self.tasks(stage_id, attempt)])

    def task_shuffle_records(self, stage_id: int, attempt: int = 0) -> DistStats:
        """Distribuição de registros de shuffle lidos por task: mostra o skew sem ruído de tempo."""
        return dist_stats(
            [t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in self.tasks(stage_id, attempt)]
        )

    def heaviest_shuffle_stage(self, group: str) -> dict[str, Any]:
        """Stage do grupo que mais leu shuffle — normalmente o do join/agregação."""
        stages = self.stages(group)
        if not stages:
            raise LookupError(f"nenhum stage encontrado para o job group {group!r}")
        return max(stages, key=lambda s: s["shuffleReadBytes"])

    # -- SQL: métricas dos operadores (scan) ----------------------------------------------------
    def sql_executions(self, group: str, include_delta_log: bool = False) -> list[dict[str, Any]]:
        """Execuções SQL (consultas) disparadas pelos jobs do grupo.

        O Delta roda consultas próprias para reconstruir o estado da tabela a partir do
        `_delta_log` (log replay). Por padrão elas ficam de fora: interessa o scan dos dados.
        """
        job_ids = {j["jobId"] for j in self.jobs(group)}
        execs = self.get("sql?details=true&planDescription=true&length=100000")
        out = []
        for e in execs:
            ids = {*e.get("successJobIds", []), *e.get("failedJobIds", []), *e.get("runningJobIds", [])}
            if not job_ids & ids:
                continue
            if not include_delta_log and "_delta_log" in e.get("planDescription", ""):
                continue
            out.append(e)
        return out

    def scan_metrics(self, group: str) -> list[dict[str, Any]]:
        """Métricas dos nós de leitura de arquivos (Scan parquet/json/...) das consultas do grupo.

        As que importam para data skipping: 'number of files read', 'size of files read',
        'number of partitions read' e 'number of output rows'.
        """
        out = []
        for e in self.sql_executions(group):
            for node in e.get("nodes", []):
                names = {m["name"] for m in node["metrics"]}
                if node["nodeName"].startswith("Scan") and "number of files read" in names:
                    metrics = {m["name"]: parse_metric_value(m["value"]) for m in node["metrics"]}
                    out.append({"execution": e["id"], "node": node["nodeName"], **metrics})
        return out


def files_read(ui: SparkUI, group: str) -> int:
    """Soma de 'number of files read' dos scans de dados do grupo (0 se não houver)."""
    return int(sum(m.get("number of files read") or 0 for m in ui.scan_metrics(group)))


# ---------------------------------------------------------------------------------------------
# Plano de execução como texto
# ---------------------------------------------------------------------------------------------


def plan_text(df: DataFrame, mode: str = "formatted") -> str:
    """O que `df.explain(mode)` imprimiria, como string (para filtrar ou comparar planos)."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def grep_plan(df: DataFrame, *keywords: str, mode: str = "simple") -> list[str]:
    """Linhas do plano que contêm alguma das palavras (ex.: 'Join', 'Exchange', 'PushedFilters')."""
    return [line.strip() for line in plan_text(df, mode).splitlines() if any(k in line for k in keywords)]


def as_table(rows: Sequence[Mapping[str, Any]], max_rows: int = 30) -> str:
    """Tabela de texto alinhada a partir de dicts (sem depender de pandas)."""
    if not rows:
        return "(vazio)"
    cols = list(rows[0].keys())
    shown = rows[:max_rows]

    def fmt(v: Any) -> str:
        if isinstance(v, float):
            return f"{v:,.2f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)

    cells = [[fmt(r.get(c)) for c in cols] for r in shown]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(cols)]
    lines = ["  ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True))]
    lines.append("  ".join("-" * w for w in widths))
    lines += ["  ".join(v.rjust(w) for v, w in zip(row, widths, strict=True)) for row in cells]
    if len(rows) > max_rows:
        lines.append(f"... (+{len(rows) - max_rows} linhas)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------
# Delta: estatísticas por arquivo (base do data skipping)
# ---------------------------------------------------------------------------------------------


def delta_active_files(table_path: str) -> list[dict[str, Any]]:
    """Arquivos ativos de uma tabela Delta local com as estatísticas min/max de cada um.

    Faz o *log replay* à mão: percorre os commits JSON em ordem; `add` liga o arquivo,
    `remove` desliga. As estatísticas (`stats`) de cada `add` são o que o Delta usa para
    pular arquivos (data skipping). Só lê os JSON — serve para tabelas de demonstração
    cujo log ainda tem a versão 0 (o leitor de verdade começa pelo checkpoint parquet).
    """
    log = Path(table_path.removeprefix("file:")) / "_delta_log"
    commits = sorted(log.glob("*.json"), key=lambda p: int(p.stem))
    if not commits or int(commits[0].stem) != 0:
        raise NotImplementedError("o log não tem mais a versão 0: é preciso começar pelo checkpoint")
    active: dict[tuple[str, str], dict[str, Any]] = {}

    def key(action: dict[str, Any]) -> tuple[str, str]:
        dv = action.get("deletionVector") or {}
        return action["path"], f"{dv.get('storageType', '')}{dv.get('pathOrInlineDv', '')}"

    for commit in commits:
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "add" in action:
                active[key(action["add"])] = action["add"]
            elif "remove" in action:
                active.pop(key(action["remove"]), None)
    out = []
    for add in active.values():
        stats = json.loads(add.get("stats") or "{}")
        out.append(
            {
                "path": add["path"],
                "size": add["size"],
                "num_records": stats.get("numRecords"),
                "min": stats.get("minValues", {}),
                "max": stats.get("maxValues", {}),
            }
        )
    return sorted(out, key=lambda f: f["path"])


# ---------------------------------------------------------------------------------------------
# Skew: salting seletivo
# ---------------------------------------------------------------------------------------------


def salted_join(
    big: DataFrame,
    small: DataFrame,
    key: str,
    hot_keys: Sequence[Any],
    buckets: int,
    how: str = "inner",
    seed: int = 7,
) -> DataFrame:
    """Join com *salting* só nas chaves quentes (hot keys).

    Lado grande: cada linha de chave quente ganha um sal aleatório 0..buckets-1 (as outras, 0).
    Lado pequeno: cada linha de chave quente é replicada `buckets` vezes (uma por sal).
    O join passa a ser por (chave, sal): a chave quente se espalha por `buckets` tasks.
    Salgar todas as chaves também funciona, mas multiplica o lado pequeno inteiro por `buckets`.
    """
    if buckets < 2:
        raise ValueError("buckets precisa ser >= 2")
    hot = F.col(key).isin(list(hot_keys))
    big_s = big.withColumn("_salt", F.when(hot, (F.rand(seed) * buckets).cast("int")).otherwise(F.lit(0)))
    salts = F.when(hot, F.sequence(F.lit(0), F.lit(buckets - 1))).otherwise(F.array(F.lit(0)))
    small_s = small.withColumn("_salt", F.explode(salts))
    return big_s.join(small_s, [key, "_salt"], how).drop("_salt")
