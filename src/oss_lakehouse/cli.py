"""Linha de comando do projeto: `python -m oss_lakehouse.cli <comando>`.

Comandos: download | bronze | silver | gold | quality | demo

São os mesmos entrypoints do job no Databricks (`resources/jobs.yml` → `python_wheel_task`).
"""

from __future__ import annotations

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

from oss_lakehouse.config import get_settings

DEMO_DAY = datetime(2026, 10, 1)
LANDING_HOURS = (12, 13, 14)


def cmd_download() -> None:
    """Baixa as 24 h do dia de demonstração do GH Archive para o cache e copia 3 delas para a landing."""
    from oss_lakehouse.sources.gharchive import download_hours

    s = get_settings()
    cache = Path(s.data_root) / "raw_cache" / "gharchive"
    landing = Path(s.path("landing", "gharchive"))
    download_hours(DEMO_DAY.replace(hour=0), DEMO_DAY.replace(hour=23), cache)
    landing.mkdir(parents=True, exist_ok=True)
    for h in LANDING_HOURS:
        name = f"{DEMO_DAY:%Y-%m-%d}-{h}.json.gz"
        if not (landing / name).exists():
            shutil.copy2(cache / name, landing / name)
    print(f"landing: {sorted(p.name for p in landing.glob('*.json.gz'))}")


def _run_step(name: str) -> None:
    from oss_lakehouse import pipeline
    from oss_lakehouse.spark import get_spark

    spark = get_spark(name)
    result = getattr(pipeline, f"run_{name}")(spark)
    print(f"{name}: {result}")


def cmd_bronze() -> None:
    """Ingere na bronze os arquivos novos da landing e imprime o total de linhas."""
    _run_step("bronze")


def cmd_silver() -> None:
    """Constrói a silver (MERGE) e a dimensão de repositório em SCD2 e imprime as contagens."""
    _run_step("silver")


def cmd_gold() -> None:
    """Publica o star schema da gold a partir da silver e imprime as contagens por tabela."""
    _run_step("gold")


def cmd_quality() -> None:
    """Valida a silver contra o contrato, aplica as expectations e grava a quarentena."""
    _run_step("quality")


def cmd_demo() -> None:
    """Roda a demonstração de ponta a ponta (`demo.run`)."""
    from oss_lakehouse import demo

    demo.run()


def main() -> None:
    """Entrypoint: configura o logging e despacha o subcomando recebido na linha de comando."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)  # a ponte Python↔JVM é tagarela em INFO
    ap = argparse.ArgumentParser(prog="oss_lakehouse")
    commands = {
        "download": cmd_download,
        "bronze": cmd_bronze,
        "silver": cmd_silver,
        "gold": cmd_gold,
        "quality": cmd_quality,
        "demo": cmd_demo,
    }
    ap.add_argument("command", choices=list(commands))
    args = ap.parse_args()
    commands[args.command]()


if __name__ == "__main__":
    main()
