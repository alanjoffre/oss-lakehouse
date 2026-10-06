"""Cliente de LLM com provedores intercambiáveis, cache por hash e saída estruturada.

Arquitetura (o pipeline só conhece a interface `LLMClient`):

    pipeline ──► CacheClient ──(hit)──► resposta gravada (jsonl versionado no Git)
                     │
                     └─(miss)──► gravador: ClaudeCLIClient | AnthropicClient ──► grava no cache

- **cache** (padrão): replay das respostas gravadas. Offline, determinístico, custo zero.
  É o que o notebook e os testes usam; miss vira erro (`CacheMiss`), nunca chamada escondida.
- **claude_cli**: subprocesso `claude -p ... --output-format json --json-schema ...`. Usado só
  para GRAVAR o cache nesta máquina (usa a sessão do Claude Code; sem chave de API no código).
- **anthropic**: SDK oficial (`messages.create` com `output_config.format` = JSON Schema).
  Caminho de produção quando existe `ANTHROPIC_API_KEY` — testado com mock.

A chave do cache é o SHA-256 de (modelo, id e versão do prompt, system, user, schema).
Mudou qualquer byte do prompt → chave nova → o teste de gate acusa "cache desatualizado".
Isso é a **idempotência por hash**: reprocessar a mesma entrada nunca paga duas vezes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ValidationError

from oss_lakehouse.ai.pricing import custo_usd
from oss_lakehouse.utils.retry import retry

log = logging.getLogger(__name__)

# Modelo leve para tarefas de classificação/extração em volume. Ver pricing.py e notebook 12.
DEFAULT_MODEL = "claude-haiku-4-5"
CACHE_PATH = Path(__file__).parent / "cache" / "respostas.jsonl"


class LLMError(RuntimeError):
    """Falha definitiva (não adianta repetir)."""


class LLMTransientError(LLMError):
    """Falha transitória (timeout, 429, 5xx): o retry com backoff tenta de novo."""


class LLMOutputError(LLMError):
    """O modelo respondeu, mas a saída não passou na validação do schema."""


class CacheMiss(LLMError):
    """Pedido sem resposta gravada, em modo somente-cache."""


@dataclass(frozen=True)
class LLMRequest:
    """Pedido já renderizado: prompt (system + user), modelo e JSON Schema da saída.

    Imutável; `key()` é o SHA-256 que serve de chave do cache — `max_tokens` fica de fora da chave.
    """

    prompt_id: str
    prompt_version: str
    system: str
    user: str
    schema: dict[str, Any]
    model: str = DEFAULT_MODEL
    max_tokens: int = 4096

    def key(self) -> str:
        payload = json.dumps(
            {
                "model": self.model,
                "prompt_id": self.prompt_id,
                "prompt_version": self.prompt_version,
                "system": self.system,
                "user": self.user,
                "schema": self.schema,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass
class Usage:
    """Consumo de uma chamada, em tokens. `cost_usd` é `None` quando o provedor não informou o custo."""

    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float | None = None


@dataclass
class LLMResponse:
    """Resposta de um provedor. `data` é o JSON ainda NÃO validado — quem valida é `completar`.

    `key` é a chave do pedido; em replay (`from_cache=True`) `latency_ms` e `usage` são os da gravação.
    """

    key: str
    data: dict[str, Any]
    usage: Usage = field(default_factory=Usage)
    provider: str = ""
    model: str = ""
    latency_ms: int | None = None
    from_cache: bool = False


class LLMClient(Protocol):
    """Interface que o pipeline conhece: qualquer objeto com `name` e `complete(request)` serve."""

    name: str

    def complete(self, request: LLMRequest) -> LLMResponse: ...


# ---------------------------------------------------------------------------
# Provedor: cache (replay)
# ---------------------------------------------------------------------------


class CacheClient:
    """Replay de respostas gravadas; com `gravador`, chama o provedor real no miss e grava."""

    name = "cache"

    def __init__(self, path: str | Path = CACHE_PATH, gravador: LLMClient | None = None) -> None:
        self.path = Path(path)
        self.gravador = gravador
        self.hits = 0
        self.misses = 0
        self._lock = threading.Lock()
        self._entradas = self._carregar()

    def _carregar(self) -> dict[str, dict[str, Any]]:
        entradas: dict[str, dict[str, Any]] = {}
        if self.path.exists():
            for linha in self.path.read_text(encoding="utf-8").splitlines():
                if linha.strip():
                    reg = json.loads(linha)
                    entradas[reg["key"]] = reg
        return entradas

    def __contains__(self, request: LLMRequest) -> bool:
        return request.key() in self._entradas

    def __len__(self) -> int:
        return len(self._entradas)

    def complete(self, request: LLMRequest) -> LLMResponse:
        k = request.key()
        reg = self._entradas.get(k)
        if reg is not None:
            self.hits += 1
            return LLMResponse(
                key=k,
                data=reg["data"],
                usage=Usage(**reg.get("usage", {})),
                provider=reg.get("provider", "?"),
                model=reg.get("model", request.model),
                latency_ms=reg.get("latency_ms"),
                from_cache=True,
            )
        self.misses += 1
        if self.gravador is None:
            raise CacheMiss(
                f"sem resposta gravada para {request.prompt_id} v{request.prompt_version} "
                f"(chave {k[:12]}). Grave com OSSLH_LLM_PROVIDER=claude_cli ou anthropic."
            )
        resp = self.gravador.complete(request)
        self._gravar(request, resp)
        return resp

    def _gravar(self, request: LLMRequest, resp: LLMResponse) -> None:
        reg = {
            "key": resp.key,
            "prompt_id": request.prompt_id,
            "prompt_version": request.prompt_version,
            "model": resp.model or request.model,
            "provider": resp.provider,
            "latency_ms": resp.latency_ms,
            "usage": asdict(resp.usage),
            "data": resp.data,
        }
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(reg, ensure_ascii=False, sort_keys=True) + "\n")
            self._entradas[resp.key] = reg


# ---------------------------------------------------------------------------
# Provedor: CLI do Claude Code (gravação)
# ---------------------------------------------------------------------------

Runner = Callable[..., subprocess.CompletedProcess[str]]


class ClaudeCLIClient:
    """`claude -p` em modo não interativo, sem ferramentas, com saída validada por JSON Schema.

    - `--safe-mode`, `--tools ""`, `--strict-mcp-config`: o modelo só responde texto — nada de
      ler disco, rodar comando ou carregar CLAUDE.md/plugins do usuário.
    - `--system-prompt` substitui o prompt padrão do Claude Code pelo nosso.
    - `--json-schema`: o CLI valida a resposta e devolve `structured_output`.
    - roda em diretório temporário para não herdar contexto do projeto.
    """

    name = "claude_cli"

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        binary: str | None = None,
        timeout_s: int = 240,
        max_budget_usd: float = 0.50,
        runner: Runner = subprocess.run,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.model = model
        self.binary = binary or os.environ.get("OSSLH_CLAUDE_BIN", str(Path.home() / ".local/bin/claude"))
        self.timeout_s = timeout_s
        self.max_budget_usd = max_budget_usd
        self.runner = runner
        self.sleep = sleep
        self.chamadas = 0

    def comando(self, request: LLMRequest) -> list[str]:
        return [
            self.binary,
            "-p",
            "--safe-mode",
            "--tools",
            "",
            "--strict-mcp-config",
            "--no-session-persistence",
            "--model",
            request.model,
            "--output-format",
            "json",
            "--max-budget-usd",
            str(self.max_budget_usd),
            "--system-prompt",
            request.system,
            "--json-schema",
            json.dumps(request.schema, ensure_ascii=False),
        ]

    def complete(self, request: LLMRequest) -> LLMResponse:
        @retry(exceptions=(LLMTransientError,), attempts=3, base_delay=5.0, sleep=self.sleep)
        def _uma_vez() -> LLMResponse:
            return self._chamar(request)

        return _uma_vez()

    def _chamar(self, request: LLMRequest) -> LLMResponse:
        self.chamadas += 1
        t0 = time.monotonic()
        try:
            proc = self.runner(
                self.comando(request),
                input=request.user,
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
                cwd=tempfile.gettempdir(),
            )
        except subprocess.TimeoutExpired as exc:
            raise LLMTransientError(f"timeout de {self.timeout_s}s no CLI") from exc
        latency = int((time.monotonic() - t0) * 1000)
        try:
            out = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            msg = (proc.stderr or proc.stdout or "")[:300]
            transitorio = any(s in msg.lower() for s in ("rate limit", "overloaded", "429", "529"))
            erro = LLMTransientError if transitorio else LLMError
            raise erro(f"CLI saiu com {proc.returncode}: {msg}") from exc
        if out.get("is_error"):
            status = out.get("api_error_status")
            erro = LLMTransientError if status in (429, 500, 502, 503, 529) else LLMError
            raise erro(f"CLI devolveu erro ({status}): {str(out.get('result'))[:300]}")
        data = out.get("structured_output")
        if data is None:
            try:
                data = json.loads(out.get("result") or "")
            except json.JSONDecodeError as exc:
                raise LLMOutputError("CLI não devolveu JSON") from exc
        u = out.get("usage") or {}
        usage = Usage(
            input_tokens=int(u.get("input_tokens", 0))
            + int(u.get("cache_creation_input_tokens", 0))
            + int(u.get("cache_read_input_tokens", 0)),
            output_tokens=int(u.get("output_tokens", 0)),
            cost_usd=out.get("total_cost_usd"),
        )
        return LLMResponse(
            key=request.key(),
            data=data,
            usage=usage,
            provider=self.name,
            model=request.model,
            latency_ms=latency,
        )


# ---------------------------------------------------------------------------
# Provedor: SDK da Anthropic (produção, com ANTHROPIC_API_KEY)
# ---------------------------------------------------------------------------


class AnthropicClient:
    """Messages API com saída estruturada (`output_config.format`). O SDK já faz retry com
    backoff em 408/409/429/5xx (`max_retries`); aqui tratamos só o que ele não trata."""

    name = "anthropic"

    def __init__(self, client: Any | None = None, max_retries: int = 4, timeout_s: float = 120.0) -> None:
        if client is None:
            import anthropic

            client = anthropic.Anthropic(max_retries=max_retries, timeout=timeout_s)
        self.client = client

    def complete(self, request: LLMRequest) -> LLMResponse:
        t0 = time.monotonic()
        resp = self.client.messages.create(
            model=request.model,
            max_tokens=request.max_tokens,
            system=request.system,
            messages=[{"role": "user", "content": request.user}],
            output_config={"format": {"type": "json_schema", "schema": request.schema}},
        )
        latency = int((time.monotonic() - t0) * 1000)
        if resp.stop_reason == "refusal":
            raise LLMError("o modelo recusou o pedido (stop_reason=refusal)")
        if resp.stop_reason == "max_tokens":
            raise LLMOutputError(
                "resposta cortada em max_tokens — aumente o limite ou o lote é grande demais"
            )
        texto = next((b.text for b in resp.content if b.type == "text"), "")
        try:
            data = json.loads(texto)
        except json.JSONDecodeError as exc:
            raise LLMOutputError("resposta não é JSON") from exc
        usage = Usage(
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            cost_usd=custo_usd(request.model, resp.usage.input_tokens, resp.usage.output_tokens),
        )
        return LLMResponse(
            key=request.key(),
            data=data,
            usage=usage,
            provider=self.name,
            model=request.model,
            latency_ms=latency,
        )


# ---------------------------------------------------------------------------
# Fábrica e validação
# ---------------------------------------------------------------------------


def get_client(provider: str | None = None, cache_path: str | Path = CACHE_PATH) -> CacheClient:
    """Cliente padrão do projeto. O cache fica SEMPRE na frente (idempotência por hash).

    `OSSLH_LLM_PROVIDER`: `cache` (padrão, offline) | `claude_cli` | `anthropic`.
    """
    provider = provider or os.environ.get("OSSLH_LLM_PROVIDER", "cache")
    if provider == "cache":
        return CacheClient(cache_path)
    if provider == "claude_cli":
        return CacheClient(cache_path, gravador=ClaudeCLIClient())
    if provider == "anthropic":
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise LLMError("provider=anthropic exige ANTHROPIC_API_KEY")
        return CacheClient(cache_path, gravador=AnthropicClient())
    raise ValueError(f"provider desconhecido: {provider}")


def completar[T: BaseModel](client: LLMClient, request: LLMRequest, modelo: type[T]) -> tuple[T, LLMResponse]:
    """Chama o LLM e valida a saída com pydantic. Saída inválida nunca entra no pipeline."""
    resp = client.complete(request)
    try:
        return modelo.model_validate(resp.data), resp
    except ValidationError as exc:
        raise LLMOutputError(f"{request.prompt_id}: saída fora do contrato: {exc.errors()[:3]}") from exc
