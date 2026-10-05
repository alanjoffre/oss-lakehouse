"""Prompts versionados em arquivo (`ai/prompts/*.toml`).

Prompt é código: vive no Git, passa por code review e tem versão. Cada arquivo tem `id`,
`version`, `system`, `user` (template com `$variavel`) e `schema` (JSON Schema da saída).

Por que `string.Template` e não `str.format`: o prompt contém JSON, e chaves `{}` do JSON
brigariam com o `format`. `$variavel` não colide.
"""

from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from string import Template
from typing import Any

from oss_lakehouse.ai.client import DEFAULT_MODEL, LLMRequest

PROMPTS_DIR = Path(__file__).parent / "prompts"


@dataclass(frozen=True)
class Prompt:
    id: str
    version: str
    descricao: str
    system: str
    user: str
    schema: dict[str, Any]

    def request(self, model: str = DEFAULT_MODEL, max_tokens: int = 4096, **variaveis: str) -> LLMRequest:
        return LLMRequest(
            prompt_id=self.id,
            prompt_version=self.version,
            system=self.system.strip(),
            user=Template(self.user.strip()).substitute(variaveis),
            schema=self.schema,
            model=model,
            max_tokens=max_tokens,
        )


@cache
def carregar_prompt(nome: str) -> Prompt:
    dados = tomllib.loads((PROMPTS_DIR / f"{nome}.toml").read_text(encoding="utf-8"))
    return Prompt(
        id=dados["id"],
        version=str(dados["version"]),
        descricao=dados.get("descricao", ""),
        system=dados["system"],
        user=dados["user"],
        schema=json.loads(dados["schema"]),
    )


def listar_prompts() -> list[Prompt]:
    return [carregar_prompt(p.stem) for p in sorted(PROMPTS_DIR.glob("*.toml"))]
