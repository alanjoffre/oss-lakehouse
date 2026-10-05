"""Cliente da API do GitHub sem rede: respostas falsas por um adapter do requests + as gravações reais."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests
from requests.adapters import HTTPAdapter

from oss_lakehouse.sources.github_api import (
    CassetteAdapter,
    CassetteMiss,
    GitHubClient,
    RateLimit,
    RateLimitExceeded,
    WatermarkStore,
    cassette_session,
    fetch_issues_incremental,
    fetch_repos,
    parse_link_header,
    wait_seconds,
)

FIXTURES = Path(__file__).parent / "fixtures" / "github_api"
RL = {"x-ratelimit-limit": "60", "x-ratelimit-remaining": "59", "x-ratelimit-used": "1",
      "x-ratelimit-reset": "1791209553", "x-ratelimit-resource": "core"}


class FakeAdapter(HTTPAdapter):
    """Responde em sequência a partir de uma lista de (status, headers, body) ou de uma função."""

    def __init__(self, script: list[tuple[int, dict, object]] | Callable[[requests.PreparedRequest], tuple]):
        super().__init__()
        self.script = script
        self.requests: list[requests.PreparedRequest] = []

    def send(self, request, **kwargs):  # type: ignore[override]
        self.requests.append(request)
        status, headers, body = self.script(request) if callable(self.script) else self.script.pop(0)
        r = requests.Response()
        r.status_code = status
        r.headers = requests.structures.CaseInsensitiveDict({**RL, **headers})
        r._content = b"" if body is None else json.dumps(body).encode()
        r.url = request.url
        r.request = request
        return r


def make_client(adapter: FakeAdapter, **kw) -> tuple[GitHubClient, list[float]]:
    s = requests.Session()
    s.mount("https://", adapter)
    sleeps: list[float] = []
    return GitHubClient(session=s, sleep=sleeps.append, **kw), sleeps


def test_parse_link_header():
    h = ('<https://api.github.com/repositories/1/issues?page=2>; rel="next", '
         '<https://api.github.com/repositories/1/issues?page=5>; rel="last"')
    assert parse_link_header(h) == {
        "next": "https://api.github.com/repositories/1/issues?page=2",
        "last": "https://api.github.com/repositories/1/issues?page=5",
    }
    assert parse_link_header(None) == {}


def test_wait_seconds_prefere_retry_after_e_depois_reset():
    assert wait_seconds(403, {"Retry-After": "7"}) == 7.0
    now = datetime.fromtimestamp(1_000, tz=UTC)
    assert wait_seconds(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1030"}, now) == 30.0
    assert wait_seconds(500, {}) is None


def test_rate_limit_from_headers():
    rl = RateLimit.from_headers(RL)
    assert rl and rl.remaining == 59 and rl.limit == 60 and rl.reset.tzinfo is UTC
    assert RateLimit.from_headers({}) is None


def test_paginacao_segue_link_ate_acabar():
    nxt = '<https://api.github.com/x?page=2>; rel="next"'
    adapter = FakeAdapter([(200, {"link": nxt}, [1, 2]), (200, {}, [3])])
    client, _ = make_client(adapter)
    pages = list(client.paginate("x", {"per_page": 2}))
    assert [p.data for p in pages] == [[1, 2], [3]]
    assert adapter.requests[1].url == "https://api.github.com/x?page=2"


def test_paginacao_respeita_max_pages():
    nxt = '<https://api.github.com/x?page=2>; rel="next"'
    adapter = FakeAdapter(lambda req: (200, {"link": nxt}, [0]))
    client, _ = make_client(adapter)
    assert len(list(client.paginate("x", max_pages=3))) == 3


def test_retry_em_5xx_e_retry_after_do_limite_secundario():
    adapter = FakeAdapter([
        (503, {}, {"message": "unavailable"}),
        (403, {"retry-after": "3"}, {"message": "You have exceeded a secondary rate limit"}),
        (200, {"etag": 'W/"abc"'}, {"id": 1}),
    ])
    client, sleeps = make_client(adapter)
    r = client.get("repos/a/b")
    assert r.status == 200 and r.etag == 'W/"abc"'
    assert len(adapter.requests) == 3
    assert 3.0 in sleeps  # esperou exatamente o que o servidor pediu


def test_cota_primaria_esgotada_com_reset_longe_nao_espera():
    far = str(int(datetime.now(UTC).timestamp()) + 3600)
    adapter = FakeAdapter([(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": far},
                            {"message": "API rate limit exceeded"})])
    client, sleeps = make_client(adapter, max_wait=60)
    with pytest.raises(RateLimitExceeded):
        client.get("repos/a/b")
    assert sleeps == []


def test_404_e_dado_nao_excecao_e_etag_vira_if_none_match(tmp_path):
    adapter = FakeAdapter(lambda req: (
        (304, {}, None) if req.headers.get("If-None-Match") else
        (404, {}, {"message": "Not Found"}) if "sumiu" in req.url else
        (200, {"etag": 'W/"v1"'}, {"id": 7, "full_name": "a/b"})
    ))
    client, _ = make_client(adapter)
    rep = fetch_repos(client, ["a/b", "x/sumiu"], tmp_path, "r1")
    assert rep.status == {"a/b": 200, "x/sumiu": 404} and len(rep.files) == 1
    env = json.loads(rep.files[0].read_text())
    assert env["data"]["id"] == 7 and env["_etag"] == 'W/"v1"' and env["_status"] == 200
    r = client.get("repos/a/b", etag=env["_etag"])
    assert r.not_modified and r.data is None


def test_token_vai_no_header_authorization():
    adapter = FakeAdapter([(200, {}, {})])
    s = requests.Session()
    s.mount("https://", adapter)
    GitHubClient(token="t0k3n", session=s).get("rate_limit")
    assert adapter.requests[0].headers["Authorization"] == "Bearer t0k3n"


def test_watermark_store_atomico_e_por_chave(tmp_path):
    st = WatermarkStore(tmp_path / "s" / "state.json")
    assert st.get("k") is None
    st.set("k", "2026-10-01T00:00:00Z", "r1")
    st.set("outra", "2026-10-02T00:00:00Z", "r1")
    assert st.get("k") == "2026-10-01T00:00:00Z"
    assert not list(tmp_path.glob("s/*.tmp"))


def test_incremental_avanca_marca_e_so_busca_o_novo(tmp_path):
    issues = [{"id": i, "updated_at": f"2026-10-01T1{i}:00:00Z"} for i in range(4)]

    def api(req):
        since = dict(p.split("=") for p in req.url.split("?")[1].split("&"))["since"].replace("%3A", ":")
        return 200, {}, [it for it in issues if it["updated_at"] >= since]

    client, _ = make_client(FakeAdapter(api))
    st = WatermarkStore(tmp_path / "state.json")
    r1 = fetch_issues_incremental(client, "a/b", tmp_path, st, "r1", "2026-10-01T00:00:00Z")
    assert r1.items == 4 and r1.new_watermark == "2026-10-01T13:00:00Z"
    issues.append({"id": 9, "updated_at": "2026-10-01T15:00:00Z"})
    r2 = fetch_issues_incremental(client, "a/b", tmp_path, st, "r2", "2026-10-01T00:00:00Z")
    # since é inclusivo: volta o item da fronteira (13h) + o novo (15h)
    assert r2.since == "2026-10-01T13:00:00Z" and r2.items == 2
    assert st.get("github_issues:a/b") == "2026-10-01T15:00:00Z"


def test_marca_nao_avanca_se_a_pagina_falha(tmp_path):
    nxt = '<https://api.github.com/x?page=2>; rel="next"'
    adapter = FakeAdapter([(200, {"link": nxt}, [{"id": 1, "updated_at": "2026-10-01T10:00:00Z"}]),
                           (500, {}, {}), (500, {}, {}), (500, {}, {}), (500, {}, {})])
    client, _ = make_client(adapter)
    st = WatermarkStore(tmp_path / "state.json")
    with pytest.raises(requests.HTTPError):
        fetch_issues_incremental(client, "a/b", tmp_path, st, "r1", "2026-10-01T00:00:00Z")
    assert st.get("github_issues:a/b") is None  # dado → estado: sem estado, a próxima execução refaz


def test_replay_das_gravacoes_reais():
    client = GitHubClient(session=cassette_session(FIXTURES, "replay"), sleep=lambda _: None)
    r = client.get("repos/dust-tt/dust")
    assert r.status == 200 and r.data["full_name"] == "dust-tt/dust" and r.etag
    assert client.rate_limit().limit == 60
    with pytest.raises(CassetteMiss):
        client.get("repos/nao/gravado")


def test_cassette_key_ignora_ordem_da_query():
    a = CassetteAdapter.key("GET", "https://api.github.com/x?b=2&a=1", None)
    b = CassetteAdapter.key("GET", "https://api.github.com/x?a=1&b=2", None)
    assert a == b != CassetteAdapter.key("GET", "https://api.github.com/x?a=1&b=2", 'W/"e"')


def _envelope(path: Path, repo_id: int, name: str, stars: int, fetched_at: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"_request_url": f"https://api.github.com/repos/{name}", "_status": 200,
                                "_fetched_at": fetched_at, "_etag": 'W/"x"',
                                "data": {"id": repo_id, "full_name": name, "stargazers_count": stars,
                                         "owner": {"login": name.split("/")[0], "type": "Organization"},
                                         "topics": ["a"], "extra": {"ignorado": True}}}))


def test_merge_repos_silver_idempotente_e_mais_recente_vence(spark, tmp_path):
    from oss_lakehouse.sources.github_api import merge_repos_silver

    land, target = tmp_path / "landing", str(tmp_path / "silver")
    _envelope(land / "repos/run=1/a__b.json", 1, "a/b", 10, "2026-10-05T10:00:00+00:00")
    _envelope(land / "repos/run=1/c__d.json", 2, "c/d", 5, "2026-10-05T10:00:00+00:00")
    _envelope(land / "repos/run=2/a__b.json", 1, "a/b-renomeado", 12, "2026-10-05T11:00:00+00:00")
    m1 = merge_repos_silver(spark, str(land), target)
    assert m1["numTargetRowsInserted"] == "2"
    rows = {r.repo_id: r for r in spark.read.format("delta").load(target).collect()}
    assert rows[1].stars == 12 and rows[1].full_name == "a/b-renomeado"  # rename: mesma chave
    m2 = merge_repos_silver(spark, str(land), target)  # reprocessar a mesma landing
    assert m2["numTargetRowsInserted"] == "0" and m2["numTargetRowsUpdated"] == "0"
    assert spark.read.format("delta").load(target).count() == 2
