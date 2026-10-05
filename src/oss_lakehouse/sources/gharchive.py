"""Fonte GH Archive: um arquivo JSON.gz por hora com todos os eventos públicos do GitHub.

Garantias do download:
- idempotente: arquivo que já está na landing não é baixado de novo;
- atômico: grava em `.tmp` e renomeia — nunca deixa arquivo pela metade para o Auto Loader ler;
- resiliente: retry com backoff em falha de rede.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

import requests

from oss_lakehouse.utils.retry import retry

BASE_URL = "https://data.gharchive.org"
log = logging.getLogger(__name__)


def hour_keys(start: datetime, end: datetime) -> Iterator[str]:
    """Chaves horárias no formato do GH Archive (hora SEM zero à esquerda): 2026-10-01-9."""
    t = start.replace(minute=0, second=0, microsecond=0)
    while t <= end:
        yield f"{t:%Y-%m-%d}-{t.hour}"
        t += timedelta(hours=1)


@retry(exceptions=(requests.RequestException,), attempts=4)
def _download(url: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    tmp.rename(dest)


def download_hours(start: datetime, end: datetime, landing_dir: str | Path) -> list[Path]:
    """Baixa as horas [start, end] para a landing. Devolve só os arquivos novos."""
    landing = Path(landing_dir)
    landing.mkdir(parents=True, exist_ok=True)
    new: list[Path] = []
    for key in hour_keys(start, end):
        dest = landing / f"{key}.json.gz"
        if dest.exists() and dest.stat().st_size > 0:
            continue
        _download(f"{BASE_URL}/{key}.json.gz", dest)
        log.info("baixado %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
        new.append(dest)
    return new
