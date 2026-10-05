"""Cliente de LLM: chave de cache, replay, gravação e provedores — tudo sem rede."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from oss_lakehouse.ai.client import (
    AnthropicClient,
    CacheClient,
    CacheMiss,
    ClaudeCLIClient,
    LLMError,
    LLMOutputError,
    LLMRequest,
    LLMResponse,
    LLMTransientError,
    Usage,
    completar,
    get_client,
)
from oss_lakehouse.ai.pricing import custo_por_milhao_de_itens, custo_usd
from oss_lakehouse.ai.prompts import carregar_prompt, listar_prompts

SCHEMA = {"type": "object", "properties": {"x": {"type": "integer"}}, "required": ["x"]}


def req(user: str = "oi", version: str = "1") -> LLMRequest:
    return LLMRequest(prompt_id="teste", prompt_version=version, system="sys", user=user, schema=SCHEMA)


class Saida(BaseModel):
    x: int


class GravadorFalso:
    name = "falso"

    def __init__(self, data: dict | None = None) -> None:
        self.chamadas = 0
        self.data = data or {"x": 1}

    def complete(self, request: LLMRequest) -> LLMResponse:
        self.chamadas += 1
        return LLMResponse(key=request.key(), data=self.data, usage=Usage(10, 2, 0.001), provider=self.name)


def test_chave_estavel_e_sensivel_a_qualquer_byte_do_prompt():
    assert req().key() == req().key()
    assert req("oi").key() != req("oi ").key()
    assert req(version="1").key() != req(version="2").key()
    assert req().key() != LLMRequest("teste", "1", "sys", "oi", SCHEMA, model="claude-opus-5-5").key()


def test_cache_somente_leitura_falha_alto_no_miss(tmp_path):
    with pytest.raises(CacheMiss, match="teste v1"):
        CacheClient(tmp_path / "c.jsonl").complete(req())


def test_cache_grava_no_miss_e_nao_paga_duas_vezes(tmp_path):
    path = tmp_path / "c.jsonl"
    gravador = GravadorFalso()
    c = CacheClient(path, gravador=gravador)
    r1 = c.complete(req())
    r2 = c.complete(req())
    assert gravador.chamadas == 1 and not r1.from_cache and r2.from_cache
    # outra instância (outro processo) lê o que foi gravado
    r3 = CacheClient(path).complete(req())
    assert r3.data == {"x": 1} and r3.usage.input_tokens == 10
    assert len(path.read_text().splitlines()) == 1


def test_completar_valida_saida_com_pydantic(tmp_path):
    ok, _ = completar(CacheClient(tmp_path / "a.jsonl", GravadorFalso()), req(), Saida)
    assert ok.x == 1
    with pytest.raises(LLMOutputError):
        completar(CacheClient(tmp_path / "b.jsonl", GravadorFalso({"x": "não é int"})), req(), Saida)


def _proc(stdout: str, rc: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=rc, stdout=stdout, stderr="")


def _saida_cli(**extra) -> str:
    base = {
        "is_error": False,
        "result": '{"x": 7}',
        "structured_output": {"x": 7},
        "total_cost_usd": 0.004,
        "usage": {"input_tokens": 100, "cache_read_input_tokens": 20, "output_tokens": 30},
    }
    base.update(extra)
    return json.dumps(base)


def test_cli_monta_comando_seguro_e_le_structured_output():
    chamadas = []

    def runner(cmd, **kw):
        chamadas.append((cmd, kw))
        return _proc(_saida_cli())

    r = ClaudeCLIClient(binary="claude", runner=runner).complete(req("pergunta"))
    cmd, kw = chamadas[0]
    assert r.data == {"x": 7} and r.usage.input_tokens == 120 and r.usage.cost_usd == 0.004
    assert cmd[:2] == ["claude", "-p"] and "--safe-mode" in cmd
    assert cmd[cmd.index("--tools") + 1] == ""  # nenhuma ferramenta: o modelo só responde
    assert json.loads(cmd[cmd.index("--json-schema") + 1]) == SCHEMA
    assert kw["input"] == "pergunta"  # o conteúdo vai por stdin, não na linha de comando


def test_cli_repete_em_429_e_desiste_em_erro_definitivo():
    respostas = [_proc(_saida_cli(is_error=True, api_error_status=429, result="rate")), _proc(_saida_cli())]
    c = ClaudeCLIClient(binary="claude", runner=lambda *a, **k: respostas.pop(0), sleep=lambda s: None)
    assert c.complete(req()).data == {"x": 7} and c.chamadas == 2

    def erro_400(*a, **k):
        return _proc(_saida_cli(is_error=True, api_error_status=400, result="bad"))

    with pytest.raises(LLMError) as exc:
        ClaudeCLIClient(binary="claude", runner=erro_400, sleep=lambda s: None).complete(req())
    assert not isinstance(exc.value, LLMTransientError)


def test_cli_timeout_e_transitorio_e_esgota_tentativas():
    def estoura(*a, **k):
        raise subprocess.TimeoutExpired(cmd="claude", timeout=1)

    c = ClaudeCLIClient(binary="claude", runner=estoura, sleep=lambda s: None)
    with pytest.raises(LLMTransientError):
        c.complete(req())
    assert c.chamadas == 3


class _MensagensFalsas:
    def __init__(self, stop_reason: str = "end_turn", texto: str = '{"x": 3}') -> None:
        self.kwargs: dict = {}
        self.stop_reason, self.texto = stop_reason, texto

    def create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(
            stop_reason=self.stop_reason,
            content=[SimpleNamespace(type="text", text=self.texto)],
            usage=SimpleNamespace(input_tokens=1000, output_tokens=200),
        )


def test_anthropic_usa_saida_estruturada_e_calcula_custo():
    msgs = _MensagensFalsas()
    r = AnthropicClient(client=SimpleNamespace(messages=msgs)).complete(req())
    assert r.data == {"x": 3}
    assert msgs.kwargs["output_config"] == {"format": {"type": "json_schema", "schema": SCHEMA}}
    assert msgs.kwargs["model"] == "claude-haiku-4-5"
    assert r.usage.cost_usd == pytest.approx(custo_usd("claude-haiku-4-5", 1000, 200))


@pytest.mark.parametrize(("stop", "erro"), [("refusal", LLMError), ("max_tokens", LLMOutputError)])
def test_anthropic_trata_recusa_e_corte(stop, erro):
    with pytest.raises(erro):
        AnthropicClient(client=SimpleNamespace(messages=_MensagensFalsas(stop))).complete(req())


def test_get_client_padrao_e_offline_e_anthropic_exige_chave(monkeypatch, tmp_path):
    monkeypatch.delenv("OSSLH_LLM_PROVIDER", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    c = get_client(cache_path=tmp_path / "c.jsonl")
    assert isinstance(c, CacheClient) and c.gravador is None
    assert isinstance(get_client("claude_cli", tmp_path / "c.jsonl").gravador, ClaudeCLIClient)
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        get_client("anthropic", tmp_path / "c.jsonl")


def test_preco_e_custo_por_milhao():
    assert custo_usd("claude-haiku-4-5", 1_000_000, 1_000_000) == pytest.approx(6.0)
    assert custo_usd("claude-haiku-4-5", 1_000_000, 0, batch=True) == pytest.approx(0.5)
    assert custo_por_milhao_de_itens("claude-sonnet-5-5", 100, 10) == pytest.approx(300.0)


def test_prompts_versionados_carregam_e_tem_schema_fechado():
    prompts = listar_prompts()
    assert {p.id for p in prompts} >= {"classificar_titulos", "classificar_pii", "triagem_falha"}
    for p in prompts:
        assert p.version and p.schema["additionalProperties"] is False
    r = carregar_prompt("triagem_falha").request(job="j", contexto="c", erro="e")
    assert "Job: j" in r.user and r.prompt_version == carregar_prompt("triagem_falha").version
