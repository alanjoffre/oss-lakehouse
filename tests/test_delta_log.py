from __future__ import annotations

import pytest

from oss_lakehouse.delta_log import (
    apply_changes,
    commit_versions,
    latest_change_per_key,
    latest_version,
    log_summary,
    physical_files,
    read_changes,
    read_commit,
    retry_on_conflict,
    summarize_commit,
)


def _rows(spark, path):
    return sorted(tuple(r) for r in spark.read.format("delta").load(path).collect())


def test_summarize_commit_conta_acoes_sem_spark():
    actions = [
        {"commitInfo": {"operation": "DELETE"}},
        {"add": {"path": "a.parquet", "size": 2048, "dataChange": True, "deletionVector": {"x": 1}}},
        {"add": {"path": "b.parquet", "size": 1024, "dataChange": True}},
        {"remove": {"path": "a.parquet", "dataChange": True}},
        {"cdc": {"path": "_change_data/c.parquet", "size": 512}},
    ]
    s = summarize_commit(actions, version=7)
    assert (s.version, s.operation) == (7, "DELETE")
    assert (s.files_added, s.files_removed, s.adds_with_dv, s.cdc_files) == (2, 1, 1, 1)
    assert s.bytes_added == 2048 + 1024 + 512
    # a.parquet foi re-adicionado com DV (mesmo caminho do remove): não é arquivo novo
    assert s.bytes_new == 1024 + 512
    assert s.data_change is True
    assert s.as_row()["add c/ DV"] == 1


def test_summarize_commit_de_optimize_nao_muda_dado():
    actions = [
        {"commitInfo": {"operation": "OPTIMIZE"}},
        {"remove": {"path": "a.parquet", "dataChange": False}},
        {"add": {"path": "c.parquet", "size": 10, "dataChange": False}},
    ]
    assert summarize_commit(actions).data_change is False


def test_log_do_delta_de_verdade(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(0, 100, numPartitions=2).write.format("delta").save(path)
    spark.range(100, 110, numPartitions=1).write.format("delta").mode("append").save(path)
    spark.sql(f"DELETE FROM delta.`{path}` WHERE id < 50")

    assert commit_versions(path) == [0, 1, 2]
    kinds = {next(iter(a)) for a in read_commit(path, 0)}
    assert {"commitInfo", "protocol", "metaData", "add"} <= kinds
    v0, v1, v2 = log_summary(path)
    assert (v0.operation, v0.files_added, v0.files_removed) == ("WRITE", 2, 0)
    assert (v1.files_added, v1.files_removed) == (1, 0)
    assert v2.operation == "DELETE" and v2.files_removed >= 1 and v2.data_change
    assert [s.version for s in log_summary(path, last=1)] == [2]
    assert latest_version(spark, path) == 2
    # O disco guarda também o arquivo que o DELETE removeu do log (até o VACUUM).
    assert physical_files(path)["parquet"] >= 3


def test_retry_on_conflict_repete_ate_dar_certo():
    calls, waits = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise KeyError("conflito")
        return "ok"

    seen = []
    out = retry_on_conflict(
        flaky, attempts=3, base_delay=0.5, sleep=waits.append, retry_on=(KeyError,),
        on_retry=lambda n, exc: seen.append(n),
    )
    assert out == "ok" and len(calls) == 3
    assert waits == [0.5, 1.0]  # backoff exponencial
    assert seen == [1, 2]


def test_retry_on_conflict_desiste_e_nao_engole_outros_erros():
    def always():
        raise KeyError("conflito")

    with pytest.raises(KeyError):
        retry_on_conflict(always, attempts=2, sleep=lambda _: None, retry_on=(KeyError,))

    calls = []

    def other():
        calls.append(1)
        raise ValueError("bug, não conflito")

    with pytest.raises(ValueError):
        retry_on_conflict(other, attempts=5, sleep=lambda _: None, retry_on=(KeyError,))
    assert len(calls) == 1  # erro que não é conflito não é repetido
    with pytest.raises(ValueError):
        retry_on_conflict(lambda: 1, attempts=0)


def test_retry_on_conflict_padrao_usa_as_excecoes_do_delta():
    from delta.exceptions import ConcurrentAppendException, MetadataChangedException

    from oss_lakehouse.delta_log import conflict_exceptions

    errors = conflict_exceptions()
    assert ConcurrentAppendException in errors
    assert MetadataChangedException not in errors


def test_latest_change_per_key_fica_com_o_estado_final(spark):
    changes = spark.createDataFrame(
        [
            (1, "a", "insert", 1),
            (1, "a", "update_preimage", 2),
            (1, "b", "update_postimage", 2),
            (2, "x", "insert", 1),
            (2, "x", "delete", 3),
            (3, "old", "delete", 4),  # overwrite: apaga e reinsere no mesmo commit
            (3, "new", "insert", 4),
        ],
        "id int, v string, _change_type string, _commit_version long",
    )
    got = {r.id: (r.v, r._change_type) for r in latest_change_per_key(changes, "id").collect()}
    assert got == {1: ("b", "update_postimage"), 2: ("x", "delete"), 3: ("new", "insert")}


def test_apply_changes_replica_incrementalmente(spark, tmp_path):
    src, dst, state = str(tmp_path / "src"), str(tmp_path / "dst"), tmp_path / "state.json"
    spark.sql(
        f"CREATE TABLE delta.`{src}` (id INT, v STRING) USING delta "
        "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )
    spark.sql(f"INSERT INTO delta.`{src}` VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    run1 = apply_changes(spark, src, dst, "id", state)
    assert (run1.start_version, run1.end_version, run1.keys_applied) == (0, 1, 3)
    assert _rows(spark, dst) == _rows(spark, src)

    # Sem nada novo: não lê nem escreve.
    run2 = apply_changes(spark, src, dst, "id", state)
    assert run2.skipped and latest_version(spark, dst) == latest_version(spark, dst)

    spark.sql(f"UPDATE delta.`{src}` SET v = 'B' WHERE id = 2")
    spark.sql(f"DELETE FROM delta.`{src}` WHERE id = 3")
    spark.sql(f"INSERT INTO delta.`{src}` VALUES (4, 'd')")
    spark.sql(f"UPDATE delta.`{src}` SET v = 'D' WHERE id = 4")  # 2 mudanças na mesma chave

    run3 = apply_changes(spark, src, dst, "id", state)
    assert (run3.start_version, run3.end_version) == (2, 5)
    assert run3.keys_applied == 3  # chaves 2, 3 e 4 — uma linha por chave
    assert _rows(spark, dst) == _rows(spark, src) == [(1, "a"), (2, "B"), (4, "D")]
    assert read_changes(spark, src, 2, 5).filter("_change_type = 'update_preimage'").count() == 2

    # Reprocessar o mesmo intervalo (queda antes de gravar a marca) dá o mesmo resultado.
    state.write_text('{"last_version": 1}')
    apply_changes(spark, src, dst, "id", state)
    assert _rows(spark, dst) == _rows(spark, src)
