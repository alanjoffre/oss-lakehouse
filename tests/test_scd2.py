from __future__ import annotations

from datetime import datetime

import pytest

from oss_lakehouse.scd2 import apply_scd2, scd2_as_of

KEYS, TRACKED = ["repo_id"], ["repo_name"]
SCHEMA = "repo_id long, repo_name string, observed_at timestamp"


def t(h: int) -> datetime:
    return datetime(2026, 10, 1, h)


@pytest.fixture
def target(tmp_path):
    return str(tmp_path / "dim_repo_scd2")


def _apply(spark, rows, target):
    return apply_scd2(spark, spark.createDataFrame(rows, SCHEMA), target, KEYS, TRACKED, "observed_at")


def _timeline(spark, target, repo_id=1):
    rows = (
        spark.read.format("delta").load(target).filter(f"repo_id = {repo_id}").orderBy("valid_from")
        .select("repo_name", "valid_from", "valid_to", "is_current").collect()
    )
    return [tuple(r) for r in rows]


def test_carga_inicial_colapsa_observacoes_iguais(spark, target):
    _apply(spark, [(1, "a/x", t(10)), (1, "a/x", t(11)), (2, "b/y", t(10))], target)
    assert _timeline(spark, target) == [("a/x", t(10), None, True)]
    assert spark.read.format("delta").load(target).count() == 2


def test_mudanca_fecha_a_versao_e_abre_outra(spark, target):
    _apply(spark, [(1, "a/x", t(10))], target)
    m = _apply(spark, [(1, "a/x-novo", t(12))], target)
    assert _timeline(spark, target) == [("a/x", t(10), t(12), False), ("a/x-novo", t(12), None, True)]
    assert int(m["numTargetRowsInserted"]) == 1 and int(m["numTargetRowsUpdated"]) == 1


def test_sem_mudanca_nao_toca_a_tabela(spark, target):
    _apply(spark, [(1, "a/x", t(10))], target)
    m = _apply(spark, [(1, "a/x", t(13))], target)
    assert _timeline(spark, target) == [("a/x", t(10), None, True)]
    assert int(m["numTargetRowsInserted"]) == 0 and int(m["numTargetRowsUpdated"]) == 0


def test_reexecucao_do_mesmo_lote_e_idempotente(spark, target):
    batch = [(1, "a/x", t(10)), (1, "a/y", t(12)), (2, "b/z", t(11))]
    _apply(spark, batch, target)
    before = sorted(map(tuple, spark.read.format("delta").load(target).drop("_updated_at").collect()))
    m = _apply(spark, batch, target)
    after = sorted(map(tuple, spark.read.format("delta").load(target).drop("_updated_at").collect()))
    assert before == after
    changed = ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")
    assert all(int(m[k]) == 0 for k in changed)


def test_dado_atrasado_no_meio_da_historia_e_encaixado(spark, target):
    _apply(spark, [(1, "a/x", t(10)), (1, "a/z", t(14))], target)
    _apply(spark, [(1, "a/y", t(12))], target)  # chega depois, mas aconteceu antes de a/z
    assert _timeline(spark, target) == [
        ("a/x", t(10), t(12), False),
        ("a/y", t(12), t(14), False),
        ("a/z", t(14), None, True),
    ]


def test_dado_atrasado_que_torna_versao_redundante_apaga_a_versao(spark, target):
    _apply(spark, [(1, "a/x", t(10)), (1, "a/y", t(14))], target)
    m = _apply(spark, [(1, "a/y", t(12))], target)  # a mudança aconteceu às 12h, não às 14h
    assert _timeline(spark, target) == [("a/x", t(10), t(12), False), ("a/y", t(12), None, True)]
    assert int(m["numTargetRowsDeleted"]) == 1


def test_surrogate_key_e_unica_e_estavel(spark, target):
    _apply(spark, [(1, "a/x", t(10)), (1, "a/y", t(12))], target)
    sks = [r.sk for r in spark.read.format("delta").load(target).orderBy("valid_from").collect()]
    assert len(set(sks)) == 2
    _apply(spark, [(1, "a/y", t(13))], target)
    assert [r.sk for r in spark.read.format("delta").load(target).orderBy("valid_from").collect()] == sks


def test_as_of_devolve_a_versao_vigente_no_instante(spark, target):
    _apply(spark, [(1, "a/x", t(10)), (1, "a/y", t(12))], target)
    dim = spark.read.format("delta").load(target)
    assert [r.repo_name for r in scd2_as_of(dim, t(11)).collect()] == ["a/x"]
    assert [r.repo_name for r in scd2_as_of(dim, t(12)).collect()] == ["a/y"]  # valid_to é exclusivo
    assert scd2_as_of(dim, t(9)).count() == 0


def test_chave_ou_data_nula_e_descartada_e_nao_quebra_a_idempotencia(spark, target):
    # NULL = NULL não casa no MERGE: sem o descarte, a linha de chave nula seria reinserida a cada execução.
    batch = [(1, "a/x", t(10)), (None, "sem/chave", t(11)), (2, "b/y", None)]
    _apply(spark, batch, target)
    m = _apply(spark, batch, target)
    dim = spark.read.format("delta").load(target)
    assert [r.repo_id for r in dim.collect()] == [1]
    assert int(m["numTargetRowsInserted"]) == 0
