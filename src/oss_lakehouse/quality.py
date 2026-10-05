"""Qualidade de dados: expectations declarativas, quarentena, contratos YAML, freshness e volume.

- *Expectation* (expectativa): regra declarada como DADO — nome + condição SQL + ação.
  Ações (mesma semântica das expectations do Lakeflow Declarative Pipelines):
  `warn` só mede; `drop` tira a linha do fluxo (aqui ela vai para a quarentena, com o motivo);
  `fail` aborta o lote inteiro.
- Condição que avalia para NULL conta como FALHA (decisão explícita; num CHECK constraint SQL o
  NULL passa — por isso "não nulo" é sempre escrito como regra própria).
- *Data contract* (contrato de dados): YAML versionado com colunas, tipos, nulos, chave, grão,
  freshness e dono — validado contra o schema REAL antes de publicar.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

Action = Literal["warn", "drop", "fail"]


# --------------------------------------------------------------------------- expectations
@dataclass(frozen=True)
class Expectation:
    name: str
    condition: str  # expressão SQL que deve ser VERDADEIRA para a linha ser válida
    action: Action = "warn"

    def passed(self) -> Column:
        return F.coalesce(F.expr(self.condition), F.lit(False))


class ExpectationFailed(RuntimeError):
    """Regra com ação `fail` foi violada: o lote não pode seguir."""

    def __init__(self, failures: dict[str, int]):
        self.failures = failures
        super().__init__(f"expectations com ação 'fail' violadas: {failures}")


@dataclass
class QualityResult:
    valid: DataFrame
    quarantine: DataFrame
    metrics: list[dict[str, object]] = field(default_factory=list)

    def metrics_df(self, spark: SparkSession) -> DataFrame:
        return spark.createDataFrame(self.metrics)


def apply_expectations(df: DataFrame, expectations: list[Expectation]) -> QualityResult:
    """Avalia todas as regras numa passada e separa válidos × quarentena.

    - `metrics`: por regra, total, falhas e taxa de aprovação (1 agregação só, não 1 por regra);
    - `quarantine`: linhas que falharam alguma regra `drop`, com `_dq_failed_rules` e `_dq_checked_at`;
    - `valid`: o resto (regras `warn` não removem nada);
    - se alguma regra `fail` falhar, levanta `ExpectationFailed` ANTES de devolver qualquer dado.
    """
    names = [e.name for e in expectations]
    if len(set(names)) != len(names):
        raise ValueError("nomes de expectation repetidos")
    flagged = df.withColumns({f"_dq_ok_{i}": e.passed() for i, e in enumerate(expectations)})
    failed_list = F.filter(
        F.array(*[F.when(~F.col(f"_dq_ok_{i}"), F.lit(e.name)) for i, e in enumerate(expectations)]),
        lambda x: x.isNotNull(),
    )
    drop_ok = [F.col(f"_dq_ok_{i}") for i, e in enumerate(expectations) if e.action in ("drop", "fail")]
    all_drop_ok = F.lit(True)
    for c in drop_ok:
        all_drop_ok = all_drop_ok & c
    flagged = flagged.withColumn("_dq_failed_rules", failed_list).withColumn("_dq_keep", all_drop_ok)

    agg = flagged.agg(
        F.count("*").alias("_total"),
        *[
            F.sum(F.when(~F.col(f"_dq_ok_{i}"), 1).otherwise(0)).alias(f"_f{i}")
            for i in range(len(expectations))
        ],
    ).collect()[0]
    total = int(agg["_total"])
    metrics: list[dict[str, object]] = []
    for i, e in enumerate(expectations):
        failed = int(agg[f"_f{i}"] or 0)
        metrics.append(
            {
                "rule": e.name,
                "action": e.action,
                "total": total,
                "failed": failed,
                "pass_rate": round(1 - failed / total, 6) if total else 1.0,
            }
        )
    fails = {m["rule"]: m["failed"] for m in metrics if m["action"] == "fail" and m["failed"]}
    if fails:
        raise ExpectationFailed(fails)  # type: ignore[arg-type]

    tmp = [f"_dq_ok_{i}" for i in range(len(expectations))]
    valid = flagged.filter("_dq_keep").drop(*tmp, "_dq_keep", "_dq_failed_rules")
    quarantine = (
        flagged.filter("NOT _dq_keep")
        .drop(*tmp, "_dq_keep")
        .withColumn("_dq_checked_at", F.current_timestamp())
    )
    return QualityResult(valid=valid, quarantine=quarantine, metrics=metrics)


def expectations_from_dicts(rows: list[dict[str, str]]) -> list[Expectation]:
    """Regras vindas de config (YAML/JSON/tabela): o pipeline não muda quando a regra muda."""
    return [Expectation(r["name"], r["condition"], r.get("action", "warn")) for r in rows]  # type: ignore[arg-type]


# --------------------------------------------------------------------------- contratos
class ColumnSpec(BaseModel):
    name: str
    type: str  # tipo Spark em simpleString: bigint, string, timestamp, date, int, boolean…
    nullable: bool = True
    description: str = ""


class Freshness(BaseModel):
    column: str
    max_delay_hours: float


class DataContract(BaseModel):
    name: str
    version: str
    owner: str
    description: str = ""
    grain: str
    primary_key: list[str]
    columns: list[ColumnSpec]
    allow_extra_columns: bool = False
    freshness: Freshness | None = None
    expectations: list[dict[str, str]] = Field(default_factory=list)


def load_contract(path: str | Path) -> DataContract:
    with Path(path).open(encoding="utf-8") as f:
        return DataContract.model_validate(yaml.safe_load(f))


class ContractViolation(RuntimeError):
    def __init__(self, contract: str, problems: list[str]):
        self.problems = problems
        super().__init__(f"contrato '{contract}' violado: " + "; ".join(problems))


def validate_schema(schema: StructType, contract: DataContract) -> list[str]:
    """Compara o schema REAL com o contrato. Devolve a lista de problemas (vazia = ok)."""
    actual = {f.name: f.dataType.simpleString() for f in schema.fields}
    problems: list[str] = []
    for col in contract.columns:
        if col.name not in actual:
            problems.append(f"coluna ausente: {col.name}")
        elif actual[col.name] != col.type:
            problems.append(f"tipo diferente em {col.name}: contrato={col.type} real={actual[col.name]}")
    if not contract.allow_extra_columns:
        declared = {c.name for c in contract.columns}
        problems += [f"coluna fora do contrato: {c}" for c in actual if c not in declared]
    return problems


def validate_data(df: DataFrame, contract: DataContract) -> list[str]:
    """Checagens de conteúdo do contrato: chave única e colunas NOT NULL (1 agregação)."""
    not_null = [c.name for c in contract.columns if not c.nullable]
    pk = contract.primary_key
    row = df.agg(
        F.count("*").alias("_n"),
        F.count_distinct(*[F.col(k) for k in pk]).alias("_pk"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"_null_{c}") for c in not_null],
    ).collect()[0]
    problems: list[str] = []
    if row["_n"] != row["_pk"]:
        problems.append(f"chave {pk} não é única: {row['_n'] - row['_pk']} linhas duplicadas")
    problems += [f"{c} tem {row[f'_null_{c}']} nulos" for c in not_null if row[f"_null_{c}"]]
    return problems


def enforce_contract(df: DataFrame, contract: DataContract, check_data: bool = True) -> None:
    """Falha (ContractViolation) se o DataFrame não cumpre o contrato. Schema primeiro: é grátis."""
    problems = validate_schema(df.schema, contract)
    if not problems and check_data:
        problems = validate_data(df, contract)
    if problems:
        raise ContractViolation(contract.name, problems)


# --------------------------------------------------------------------------- freshness e volume
@dataclass(frozen=True)
class FreshnessResult:
    latest: datetime | None
    lag_hours: float | None
    max_delay_hours: float
    ok: bool


def check_freshness(df: DataFrame, column: str, max_delay_hours: float, now: datetime) -> FreshnessResult:
    """O dado mais novo tem no máximo `max_delay_hours` de atraso em relação a `now`?

    `now` sem fuso é tratado como UTC. A comparação é feita em *epoch* (segundos desde 1970): coletar o
    timestamp como `datetime` devolveria a hora no fuso LOCAL do driver e o atraso sairia errado em
    qualquer máquina fora de UTC. `latest` é devolvido em UTC, sem fuso.
    """
    epoch = df.agg(F.max(column).cast("double")).collect()[0][0]
    if epoch is None:
        return FreshnessResult(None, None, max_delay_hours, False)
    now_utc = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    latest = datetime.fromtimestamp(epoch, UTC).replace(tzinfo=None)
    lag = (now_utc.timestamp() - epoch) / 3600
    return FreshnessResult(latest, round(lag, 2), max_delay_hours, lag <= max_delay_hours)


def volume_anomalies(counts: dict[str, int], z_threshold: float = 3.0) -> list[dict[str, float | str | bool]]:
    """z-score de cada ponto contra os DEMAIS (leave-one-out): a anomalia não infla a própria média.

    Devolve uma linha por ponto com média/desvio de referência, z e se é anomalia.
    """
    out: list[dict[str, float | str | bool]] = []
    for key, value in counts.items():
        others = [v for k, v in counts.items() if k != key]
        if len(others) < 2:
            raise ValueError("preciso de pelo menos 3 pontos para um z-score")
        mean, stdev = statistics.fmean(others), statistics.stdev(others)
        z = (value - mean) / stdev if stdev else (0.0 if value == mean else math.inf)
        out.append(
            {"key": key, "value": value, "mean": round(mean, 1), "stdev": round(stdev, 1),
             "z": round(z, 2), "anomaly": abs(z) > z_threshold}
        )
    return out
