"""Fonte API REST do GitHub: cliente HTTP "de produção" + ingestão incremental com marca d'água.

O que este módulo resolve (e que todo pipeline que consome API precisa resolver):
- **paginação** pelo header `Link` (RFC 8288) — nunca montar `?page=N` na mão;
- **rate limit** lido dos headers `x-ratelimit-*` a cada resposta;
- **requisição condicional** com ETag (`If-None-Match` → 304 Not Modified, sem corpo);
- **retry** com backoff em 5xx/429 e no limite secundário (`Retry-After`), via `utils.retry`;
- **timeout** sempre (requests sem timeout espera para sempre);
- **landing bruta**: cada página vira um arquivo JSON com envelope de linhagem;
- **incremental** com high-water mark persistida (`since=`), gravada só depois da landing.

Para rodar offline existe o `CassetteAdapter`: grava respostas reais em disco e depois as serve
(como o vcrpy). O cliente não sabe se está falando com a API ou com a fixture.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict

from oss_lakehouse.utils.retry import retry

API_URL = "https://api.github.com"
API_VERSION = "2022-11-28"
USER_AGENT = "oss-lakehouse-study/0.1 (+https://github.com/alanjoffre/oss-lakehouse)"
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
# Headers que importam para o pipeline (o resto é ruído na fixture).
KEPT_HEADERS = ("content-type", "date", "etag", "last-modified", "link", "retry-after",
                "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-used",
                "x-ratelimit-reset", "x-ratelimit-resource")

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- modelos


@dataclass(frozen=True)
class RateLimit:
    """Estado da cota informado pelo servidor na própria resposta."""

    limit: int
    remaining: int
    used: int
    reset: datetime
    resource: str = "core"

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> RateLimit | None:
        h = CaseInsensitiveDict(headers)
        if "x-ratelimit-remaining" not in h:
            return None
        return cls(
            limit=int(h.get("x-ratelimit-limit", 0)),
            remaining=int(h["x-ratelimit-remaining"]),
            used=int(h.get("x-ratelimit-used", 0)),
            reset=datetime.fromtimestamp(int(h.get("x-ratelimit-reset", 0)), tz=UTC),
            resource=h.get("x-ratelimit-resource", "core"),
        )


@dataclass
class ApiResponse:
    status: int
    url: str
    data: Any
    headers: CaseInsensitiveDict[str]
    fetched_at: datetime

    @property
    def not_modified(self) -> bool:
        return self.status == 304

    @property
    def etag(self) -> str | None:
        return self.headers.get("etag")

    @property
    def rate_limit(self) -> RateLimit | None:
        return RateLimit.from_headers(self.headers)

    @property
    def links(self) -> dict[str, str]:
        return parse_link_header(self.headers.get("link"))


class RetryableHTTPError(requests.HTTPError):
    """Falha transitória (5xx, 429, limite secundário): vale tentar de novo."""


class CassetteMiss(requests.RequestException):
    """Replay sem gravação para a requisição: erro explícito, não retentável."""


class RateLimitExceeded(RuntimeError):
    """Cota primária esgotada e o reset está longe demais para esperar dentro do job."""

    def __init__(self, reset: datetime):
        super().__init__(f"cota do GitHub esgotada até {reset:%H:%M:%S} UTC")
        self.reset = reset


# --------------------------------------------------------------------------- helpers puros

_LINK_RE = re.compile(r'<([^>]+)>\s*;\s*rel="([^"]+)"')


def parse_link_header(value: str | None) -> dict[str, str]:
    """`<url?page=2>; rel="next", <url?page=9>; rel="last"` → {"next": url, "last": url}."""
    if not value:
        return {}
    return {rel: url for url, rel in _LINK_RE.findall(value)}


def wait_seconds(status: int, headers: Mapping[str, str], now: datetime | None = None) -> float | None:
    """Quanto esperar antes de tentar de novo, segundo o próprio servidor (None = não sabe).

    Ordem da documentação do GitHub: `Retry-After` primeiro (limite secundário);
    senão, se `x-ratelimit-remaining` é 0, esperar até `x-ratelimit-reset`.
    """
    h = CaseInsensitiveDict(headers)
    if "retry-after" in h:
        try:
            return max(0.0, float(h["retry-after"]))
        except ValueError:
            return None
    if status in (403, 429) and h.get("x-ratelimit-remaining") == "0" and "x-ratelimit-reset" in h:
        now = now or datetime.now(UTC)
        return max(0.0, int(h["x-ratelimit-reset"]) - now.timestamp())
    return None


def canonical_url(url: str, params: Mapping[str, Any] | None = None) -> str:
    """URL com a query ordenada — a mesma requisição sempre gera a mesma chave."""
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query.update({k: str(v) for k, v in (params or {}).items() if v is not None})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(sorted(query.items())), ""))


# --------------------------------------------------------------------------- cliente


class GitHubClient:
    """Cliente mínimo e robusto da API REST do GitHub.

    `token` vem de `OSSLH_GITHUB_TOKEN` (Key Vault no Databricks); sem token a cota é 60 req/h por IP.
    `sleep` é injetável para teste.
    """

    def __init__(
        self,
        token: str | None = None,
        session: requests.Session | None = None,
        base_url: str = API_URL,
        timeout: tuple[float, float] = (5.0, 30.0),
        attempts: int = 4,
        max_wait: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.attempts = attempts
        self.max_wait = max_wait
        self._sleep = sleep
        self.session = session or requests.Session()
        self.session.headers.update(
            {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": API_VERSION,
             "User-Agent": USER_AGENT}
        )
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.last_rate_limit: RateLimit | None = None
        self.calls = 0

    # -- uma tentativa ----------------------------------------------------------------
    def _send_once(self, url: str, etag: str | None) -> ApiResponse:
        headers = {"If-None-Match": etag} if etag else {}
        self.calls += 1
        r = self.session.get(url, headers=headers, timeout=self.timeout)
        rl = RateLimit.from_headers(r.headers)
        if rl:
            self.last_rate_limit = rl

        if r.status_code in RETRYABLE_STATUS or (r.status_code == 403 and _is_rate_limited(r)):
            wait = wait_seconds(r.status_code, r.headers)
            if wait is not None and wait > self.max_wait:
                raise RateLimitExceeded(rl.reset if rl else datetime.now(UTC) + timedelta(seconds=wait))
            if wait:
                log.warning("GitHub pediu espera de %.0fs (%s)", wait, r.status_code)
                self._sleep(wait)
            raise RetryableHTTPError(f"{r.status_code} em {url}", response=r)
        if r.status_code not in (200, 304, 404, 410, 451):
            r.raise_for_status()

        data = r.json() if r.status_code == 200 and r.content else None
        return ApiResponse(r.status_code, url, data, CaseInsensitiveDict(r.headers), _server_time(r.headers))

    # -- API pública ---------------------------------------------------------------------
    def get(self, path_or_url: str, params: Mapping[str, Any] | None = None,
            etag: str | None = None) -> ApiResponse:
        """GET com retry. 404 volta como resposta (repositório apagado é dado, não exceção)."""
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}/{path_or_url.lstrip('/')}"
        url = canonical_url(url, params)
        send = retry(
            exceptions=(RetryableHTTPError, requests.ConnectionError, requests.Timeout),
            attempts=self.attempts, base_delay=1.0, max_delay=30.0, sleep=self._sleep,
        )(self._send_once)
        return send(url, etag)

    def paginate(self, path: str, params: Mapping[str, Any] | None = None,
                 max_pages: int | None = None) -> Iterator[ApiResponse]:
        """Segue `rel="next"` do header Link até acabar (ou até `max_pages`, o freio de cota)."""
        resp = self.get(path, params)
        pages = 1
        yield resp
        while (nxt := resp.links.get("next")) and (max_pages is None or pages < max_pages):
            resp = self.get(nxt)
            pages += 1
            yield resp

    def rate_limit(self) -> RateLimit:
        """`GET /rate_limit` — o GitHub não desconta esta chamada da cota."""
        r = self.get("rate_limit")
        core = r.data["resources"]["core"]
        return RateLimit(core["limit"], core["remaining"], core["used"],
                         datetime.fromtimestamp(core["reset"], tz=UTC), "core")


def _server_time(headers: Mapping[str, str]) -> datetime:
    """Momento da resposta pelo relógio do SERVIDOR (header Date); no replay, o da gravação."""
    try:
        return parsedate_to_datetime(headers["date"]).astimezone(UTC)
    except (KeyError, TypeError, ValueError):
        return datetime.now(UTC)


def _is_rate_limited(r: requests.Response) -> bool:
    if "retry-after" in r.headers or r.headers.get("x-ratelimit-remaining") == "0":
        return True
    return "rate limit" in r.text.lower()


def client_from_settings(session: requests.Session | None = None, **kw: Any) -> GitHubClient:
    """Token de `OSSLH_GITHUB_TOKEN` (nunca no código; no Databricks vem do secret scope)."""
    from oss_lakehouse.config import get_settings

    return GitHubClient(token=get_settings().github_token, session=session, **kw)


# --------------------------------------------------------------------------- cassete (offline)


class CassetteAdapter(HTTPAdapter):
    """Adapter do `requests` que grava (`record`) ou reproduz (`replay`) respostas em JSON.

    Chave = método + URL canônica + `If-None-Match`. Em replay, requisição sem gravação vira erro
    explícito (`CassetteMiss`) — nunca resposta inventada.
    """

    def __init__(self, cassette_dir: str | Path, mode: Literal["record", "replay"] = "replay") -> None:
        super().__init__()
        self.dir = Path(cassette_dir)
        self.mode = mode

    @staticmethod
    def key(method: str, url: str, if_none_match: str | None) -> str:
        raw = f"{method} {canonical_url(url)} {if_none_match or ''}"
        parts = urlsplit(url)
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", parts.path.strip("/"))[:60]
        return f"{slug}__{hashlib.sha1(raw.encode()).hexdigest()[:10]}.json"

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        inm = request.headers.get("If-None-Match")
        path = self.dir / self.key(request.method or "GET", request.url or "", inm)
        if self.mode == "record":
            resp = super().send(request, **kwargs)
            self.dir.mkdir(parents=True, exist_ok=True)
            body = resp.json() if resp.content and resp.status_code != 304 else None
            path.write_text(json.dumps({
                "request": {"method": request.method, "url": canonical_url(request.url or ""),
                            "if_none_match": inm},
                "status": resp.status_code,
                "headers": {k: resp.headers[k] for k in KEPT_HEADERS if k in resp.headers},
                "body": body,
            }, ensure_ascii=False, indent=1))
            return resp
        if not path.exists():
            raise CassetteMiss(f"sem gravação para {request.method} {request.url}")
        rec = json.loads(path.read_text())
        resp = requests.Response()
        resp.status_code = rec["status"]
        resp.headers = CaseInsensitiveDict(rec["headers"])
        resp._content = b"" if rec["body"] is None else json.dumps(rec["body"]).encode()
        resp.url = request.url or ""
        resp.request = request
        resp.reason = "replay"
        return resp


def cassette_session(cassette_dir: str | Path, mode: Literal["record", "replay"]) -> requests.Session:
    s = requests.Session()
    adapter = CassetteAdapter(cassette_dir, mode)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# --------------------------------------------------------------------------- landing


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)  # leitor nunca vê arquivo pela metade


def write_landing(resp: ApiResponse, path: Path) -> Path:
    """Grava a página BRUTA com envelope de linhagem (de onde veio, quando, qual versão)."""
    envelope = {
        "_request_url": resp.url,
        "_status": resp.status,
        "_fetched_at": resp.fetched_at.isoformat(),
        "_etag": resp.etag,
        "_link": resp.headers.get("link"),
        "data": resp.data,
    }
    _atomic_write(path, json.dumps(envelope, ensure_ascii=False))
    return path


@dataclass
class FetchReport:
    files: list[Path] = field(default_factory=list)
    status: dict[str, int] = field(default_factory=dict)
    calls: int = 0
    pages: int = 0
    items: int = 0
    since: str | None = None
    new_watermark: str | None = None


def fetch_repos(client: GitHubClient, full_names: list[str], landing_dir: str | Path,
                run_id: str) -> FetchReport:
    """Metadados de cada repositório → `landing/github_api/repos/run=<id>/<owner>__<repo>.json`.

    404 (apagado/privado) e 451 (bloqueio legal) entram no relatório e não derrubam o lote.
    """
    rep = FetchReport()
    calls0 = client.calls
    for name in full_names:
        resp = client.get(f"repos/{name}")
        rep.status[name] = resp.status
        if resp.status == 200:
            out = Path(landing_dir) / "repos" / f"run={run_id}" / f"{name.replace('/', '__')}.json"
            rep.files.append(write_landing(resp, out))
    rep.calls = client.calls - calls0
    return rep


# --------------------------------------------------------------------------- marca d'água


class WatermarkStore:
    """Estado incremental em JSON, gravado de forma atômica. Chave = (fonte, entidade).

    Em produção: tabela Delta de controle (ACID, auditável) ou o próprio checkpoint do job.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _load(self) -> dict[str, dict[str, str]]:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, key: str) -> str | None:
        return self._load().get(key, {}).get("watermark")

    def set(self, key: str, watermark: str, run_id: str) -> None:
        state = self._load()
        state[key] = {"watermark": watermark, "run_id": run_id,
                      "committed_at": datetime.now(UTC).isoformat(timespec="seconds")}
        _atomic_write(self.path, json.dumps(state, indent=1, sort_keys=True))


def _iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fetch_issues_incremental(
    client: GitHubClient,
    repo: str,
    landing_dir: str | Path,
    state: WatermarkStore,
    run_id: str,
    initial_since: str,
    per_page: int = 100,
    max_pages: int = 3,
    lookback: timedelta = timedelta(minutes=0),
) -> FetchReport:
    """Issues/PRs alterados desde a marca d'água, em ordem de `updated_at` crescente.

    Por que `sort=updated&direction=asc`: se o freio `max_pages` cortar a execução, a marca
    d'água avançou só até onde de fato chegou — a próxima execução continua dali (backfill em
    fatias). Ordem decrescente + corte = buraco silencioso.
    A marca só é gravada DEPOIS de todas as páginas estarem na landing (ordem: dado → estado).
    `since` do GitHub é inclusivo (>=): o item da fronteira volta, e o MERGE por id absorve.
    """
    key = f"github_issues:{repo}"
    wm = state.get(key) or initial_since
    since_dt = _iso(wm) - lookback
    since = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    rep = FetchReport(since=since)
    calls0 = client.calls
    max_seen: str | None = None
    params = {"state": "all", "sort": "updated", "direction": "asc", "since": since, "per_page": per_page}
    for i, resp in enumerate(client.paginate(f"repos/{repo}/issues", params, max_pages=max_pages), 1):
        items = resp.data or []
        out = Path(landing_dir) / "issues" / repo.replace("/", "__") / f"run={run_id}" / f"page={i:04d}.json"
        rep.files.append(write_landing(resp, out))
        rep.pages += 1
        rep.items += len(items)
        for it in items:
            if max_seen is None or _iso(it["updated_at"]) > _iso(max_seen):
                max_seen = it["updated_at"]
    rep.calls = client.calls - calls0
    if max_seen and _iso(max_seen) > _iso(wm):
        state.set(key, max_seen, run_id)
    rep.new_watermark = state.get(key) or wm
    return rep


# --------------------------------------------------------------------------- seleção (Spark)


def select_active_repos(bronze_df: Any, n: int = 18) -> list[str]:
    """Os `n` repositórios com mais pessoas distintas (bots fora) na bronze do GH Archive.

    Distintos > eventos: um único bot fazendo 5 mil pushes não é "repositório ativo".
    Desempate determinístico (eventos, nome) — a mesma bronze sempre gera a mesma lista.
    """
    from pyspark.sql import functions as F

    return [
        r["name"]
        for r in (
            bronze_df.where(~F.col("actor.login").endswith("[bot]"))
            .groupBy(F.col("repo.name").alias("name"))
            .agg(F.countDistinct("actor.id").alias("actors"), F.count("*").alias("events"))
            .orderBy(F.desc("actors"), F.desc("events"), F.asc("name"))
            .limit(n)
            .collect()
        )
    ]


# --------------------------------------------------------------------------- landing → silver (Spark)

# Só o que a Silver usa: schema explícito ignora o resto do JSON (que cresce sem aviso na API).
_REPO_FIELDS = (
    "id long, full_name string, owner struct<login: string, type: string>, description string, "
    "language string, license struct<spdx_id: string>, topics array<string>, stargazers_count long, "
    "forks_count long, open_issues_count long, subscribers_count long, size long, default_branch string, "
    "archived boolean, fork boolean, created_at string, updated_at string, pushed_at string"
)
REPO_ENVELOPE_DDL = (
    "_request_url string, _status int, _fetched_at string, _etag string, _link string, "
    f"data struct<{_REPO_FIELDS}>"
)


def repos_from_landing(spark: Any, landing_dir: str) -> Any:
    """Envelopes de `landing/github_api/repos/run=*/` → linhas tipadas de `silver.github_repos`.

    Grão: 1 linha por repositório (`repo_id`, o mesmo `repo.id` do GH Archive — estável em rename).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    raw = (spark.read.schema(REPO_ENVELOPE_DDL).option("multiLine", True)
           .json(f"{landing_dir}/repos/run=*/*.json").select("*", "_metadata"))
    d = F.col("data")
    rows = raw.where(F.col("_status") == 200).select(
        d.id.alias("repo_id"),
        d.full_name.alias("full_name"),
        d.owner.login.alias("owner"),
        d.owner.type.alias("owner_type"),
        d.description.alias("description"),
        d.language.alias("language"),
        d.license.spdx_id.alias("license"),
        d.topics.alias("topics"),
        d.stargazers_count.alias("stars"),
        d.forks_count.alias("forks"),
        d.open_issues_count.alias("open_issues"),
        d.subscribers_count.alias("watchers"),
        d.size.alias("size_kb"),
        d.default_branch.alias("default_branch"),
        d.archived.alias("is_archived"),
        d.fork.alias("is_fork"),
        F.to_timestamp(d.created_at).alias("created_at"),
        F.to_timestamp(d.pushed_at).alias("pushed_at"),
        F.to_timestamp(d.updated_at).alias("api_updated_at"),
        F.to_timestamp("_fetched_at").alias("_fetched_at"),
        F.col("_etag"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
    # Mais de uma captura do mesmo repo na landing: fica a mais recente (dedupe ANTES do MERGE,
    # senão o MERGE falha com "multiple source rows matched").
    w = Window.partitionBy("repo_id").orderBy(F.desc("_fetched_at"))
    return rows.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def merge_repos_silver(spark: Any, landing_dir: str, target_path: str) -> dict[str, str]:
    """MERGE (upsert) idempotente da landing na Silver. Devolve as métricas da operação.

    `WHEN MATCHED AND s._fetched_at > t._fetched_at`: captura velha reprocessada não sobrescreve
    a nova (proteção contra dado fora de ordem).
    """
    from delta.tables import DeltaTable

    src = repos_from_landing(spark, landing_dir)
    if not DeltaTable.isDeltaTable(spark, target_path):
        src.limit(0).write.format("delta").save(target_path)
    (DeltaTable.forPath(spark, target_path).alias("t")
     .merge(src.alias("s"), "t.repo_id = s.repo_id")
     .whenMatchedUpdateAll(condition="s._fetched_at > t._fetched_at")
     .whenNotMatchedInsertAll()
     .execute())
    last = DeltaTable.forPath(spark, target_path).history(1).collect()[0]
    return dict(last["operationMetrics"])
