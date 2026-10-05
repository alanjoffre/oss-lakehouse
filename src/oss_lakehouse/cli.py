"""Linha de comando do projeto: `python -m oss_lakehouse.cli <comando>`.

Comandos: download | bronze | demo
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


def cmd_bronze() -> None:
    from oss_lakehouse.bronze import BRONZE_TABLE, ingest_gharchive_bronze
    from oss_lakehouse.spark import get_spark

    spark = get_spark("bronze")
    ingest_gharchive_bronze(spark)
    n = spark.read.format("delta").load(get_settings().path("bronze", BRONZE_TABLE)).count()
    print(f"bronze.{BRONZE_TABLE}: {n:,} linhas")


def cmd_demo() -> None:
    from oss_lakehouse import demo

    demo.run()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(prog="oss_lakehouse")
    ap.add_argument("command", choices=["download", "bronze", "demo"])
    args = ap.parse_args()
    {"download": cmd_download, "bronze": cmd_bronze, "demo": cmd_demo}[args.command]()


if __name__ == "__main__":
    main()
