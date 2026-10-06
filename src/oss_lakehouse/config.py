"""Configuração única do projeto: caminhos por ambiente (local ou Databricks/Azure).

Por que um módulo de config: o mesmo código roda no laptop (pastas locais) e no
Databricks (ADLS Gen2 via `abfss://`). O que muda é só a raiz — nunca o código do pipeline.
Variáveis de ambiente com prefixo `OSSLH_` sobrescrevem os padrões (12-factor app).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]

Layer = Literal["landing", "bronze", "silver", "gold", "quarantine"]


class Settings(BaseSettings):
    """Configuração do projeto, lida de variáveis `OSSLH_*` e do `.env` (chave desconhecida é ignorada)."""

    model_config = SettingsConfigDict(env_prefix="OSSLH_", env_file=".env", extra="ignore")

    env: Literal["local", "databricks"] = "local"
    # Local: pasta data/ do repositório. Databricks: abfss://lake@<conta>.dfs.core.windows.net
    data_root: str = str(PROJECT_ROOT / "data")
    spark_master: str = "local[4]"
    spark_driver_memory: str = "2g"
    shuffle_partitions: int = 8
    github_token: str | None = None

    def path(self, layer: Layer, *parts: str) -> str:
        """Caminho de uma camada do lakehouse: path('silver', 'gh_events')."""
        return "/".join([self.data_root.rstrip("/"), layer, *parts])

    def checkpoint(self, name: str) -> str:
        """Checkpoint de streaming — um por consulta, nunca compartilhado."""
        return "/".join([self.data_root.rstrip("/"), "_checkpoints", name])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Instância única de `Settings`: o ambiente é lido uma vez por processo (`lru_cache`).

    `get_settings.cache_clear()` força a releitura.
    """
    return Settings()
