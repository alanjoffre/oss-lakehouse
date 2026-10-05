from __future__ import annotations

import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Uma sessão Spark para a suíte inteira (subir a JVM custa segundos)."""
    os.environ.setdefault("OSSLH_DATA_ROOT", str(tmp_path_factory.mktemp("lake")))
    os.environ.setdefault("OSSLH_SPARK_MASTER", "local[2]")
    os.environ.setdefault("OSSLH_SHUFFLE_PARTITIONS", "2")
    from oss_lakehouse.spark import get_spark

    session = get_spark("tests")
    yield session
    session.stop()


@pytest.fixture
def gh_sample_dir() -> Path:
    """Pasta com uma amostra real do GH Archive (2.000 eventos) para testes de ponta a ponta."""
    return FIXTURES / "gharchive"
