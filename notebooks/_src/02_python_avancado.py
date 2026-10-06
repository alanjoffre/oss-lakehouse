# %% [markdown]
# # 02 · Python avançado para engenharia de dados
#
# > Prova, com os arquivos reais do GH Archive, que o Python de um engenheiro de dados sênior é medido em
# > memória, tempo e falhas tratadas — não em sintaxe: generators com memória constante, decorators e
# > context managers que viram infraestrutura, typing que documenta contrato, validação com pydantic,
# > concorrência escolhida pelo tipo de gargalo, e testes.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Python avançado | §1–§10 (generators, itertools, decorators, context managers, typing, dataclasses × pydantic, functools, concorrência, erros e logs) |
# | Processamento de dados | §1 (arquivo de 18 MB com memória constante), §8 (paralelismo medido), §13–§14 (Python × Spark, UDF) |
# | Desenvolvimento de pipelines | §3 (retry), §4 (escrita atômica), §10 (log estruturado), §11 (wheel) |
# | Git/versionamento e qualidade | §11 (`pyproject.toml`, uv, lock), §12 (pytest) |
# | Databricks | §13–§14 e "No Databricks / Azure" (wheel no cluster, pandas UDF) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup
#
# O Spark só sobe na §13. Motivo: a §8 cria processos com `fork`, e fazer *fork* de um processo que já
# tem uma JVM rodando é pedir problema (threads da JVM não sobrevivem ao fork). Ordem importa.

# %%
import gzip
import json
import logging
import sys
import time
import tracemalloc
from pathlib import Path

from oss_lakehouse.config import get_settings

s = get_settings()
LANDING = Path(s.path("landing", "gharchive"))
CACHE = Path(s.data_root) / "raw_cache" / "gharchive"
DEMO = Path(s.data_root) / "demo" / "02"
DEMO.mkdir(parents=True, exist_ok=True)
ARQ = LANDING / "2026-10-01-12.json.gz"  # 1 hora de eventos públicos do GitHub

print(sys.version.split()[0], "|", ARQ.name, f"{ARQ.stat().st_size / 1e6:.1f} MB comprimido")

# %% [markdown]
# ## 1. Generators e iterators — processar um arquivo com memória constante 🧪
#
# **O que é** — um *iterator* é qualquer objeto que entrega um item por vez (`__next__`). Um *generator*
# é a forma mais fácil de escrever um: uma função com `yield`. Ela pausa a cada item e só calcula o
# próximo quando alguém pede (avaliação preguiçosa, *lazy evaluation*).
#
# **Por que importa** — arquivo de dados não cabe na memória por definição (um dia de GH Archive tem
# ~500 MB comprimidos). `json.load(arquivo_inteiro)` ou `list(...)` faz o pico de memória crescer com o
# arquivo; um generator mantém o pico do tamanho de **uma linha**.
#
# **Como funciona** — `iter_jsonl_gz` (em `src/oss_lakehouse/utils/io.py`) abre o `.gz` em modo texto
# (o `gzip` descomprime em blocos) e faz `yield json.loads(linha)`. Quem consome decide o ritmo:
#
# ```text
# arquivo.gz ─(bloco 32 KB)→ gzip ─(linha)→ json.loads ─(dict)→ consumidor ─(descarta)→ próximo
# ```
#
# Medimos com `tracemalloc` (rastreia toda alocação do Python e devolve o pico) as duas abordagens
# sobre o **mesmo arquivo inteiro**.

# %%
from oss_lakehouse.utils.io import iter_jsonl_gz


def pico_mb(fn):
    """Executa fn() e devolve (resultado, pico de memória Python em MB, segundos)."""
    tracemalloc.start()
    t0 = time.perf_counter()
    out = fn()
    secs = time.perf_counter() - t0
    _, pico = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return out, pico / 1e6, secs


def carregar_tudo():
    with gzip.open(ARQ, "rt") as f:
        eventos = [json.loads(linha) for linha in f]  # lista com TODOS os dicts
    return sum(1 for e in eventos if e["type"] == "PushEvent")


def em_streaming():
    return sum(1 for e in iter_jsonl_gz(ARQ) if e["type"] == "PushEvent")


push_lista, mb_lista, t_lista = pico_mb(carregar_tudo)
push_gen, mb_gen, t_gen = pico_mb(em_streaming)
assert push_lista == push_gen
print(f"PushEvent na hora: {push_gen:,}")
print(f"lista inteira : pico {mb_lista:8.1f} MB  ({t_lista:.1f}s com tracemalloc ligado)")
print(f"generator     : pico {mb_gen:8.1f} MB  ({t_gen:.1f}s com tracemalloc ligado)")
print(f"→ {mb_lista / mb_gen:,.0f}× menos memória, mesmo resultado")

# %% [markdown]
# O pico do generator não depende do tamanho do arquivo; o da lista cresce linearmente com ele —
# um dia inteiro seria ~24× o pico acima. (O `tracemalloc` deixa o código mais lento; os tempos aqui só
# servem para comparar entre si.)
#
# Um generator é consumido **uma vez só** — esta é a pegadinha mais comum:

# %%
gen = (e["type"] for e in iter_jsonl_gz(ARQ))
primeiros = [next(gen) for _ in range(3)]
print("3 primeiros:", primeiros)
print("o generator continua de onde parou:", next(gen))
gen.close()  # fecha o arquivo por baixo (dispara o finally/with dentro do generator)

esgotado = iter([1, 2, 3])
print("1ª passada:", list(esgotado), "| 2ª passada:", list(esgotado))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Generator é uma função com `yield` que produz um item por vez sob demanda.
# > Uso para ler arquivo e paginar API: a memória fica do tamanho de um item, não do arquivo. Medi aqui
# > num arquivo real de 1 hora do GH Archive: a lista inteira chega a centenas de MB de pico, o generator
# > fica abaixo de 1 MB. Cuidado: ele só pode ser percorrido uma vez."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Protocolo:** `iter(x)` chama `x.__iter__()`; o `for` chama `__next__()` até `StopIteration`.
#   Generator implementa os dois e ainda `send()`, `throw()` e `close()`.
# - **`yield from`:** delega para outro iterável (usado em `iter_jsonl_gz_many` para encadear arquivos).
# - **Limpeza:** o `with` dentro do generator só fecha o arquivo quando o generator termina ou é
#   coletado/fechado. Generator abandonado no meio segura o arquivo aberto — feche com `.close()` ou
#   use `contextlib.closing`.
# - **Generator expression** `(x for x in ...)` × list comprehension `[...]`: parênteses = lazy.
# - **No Spark** não se itera linha a linha no driver: quem paraleliza é o executor. Generators aparecem
#   em `mapPartitions`/`foreachPartition` (processar uma partição sem materializá-la) e nos pandas UDF de
#   iterador (`Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]`).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Precisa percorrer os dados 2× (ex.: média e depois desvio)? Ou guarda numa lista, ou relê a fonte.
# - Precisa de ordenação global, join ou `groupBy` em dados maiores que a memória? Generator não resolve —
#   é caso de agregação externa (§ Python puro do notebook 16) ou de Spark.
# - Depurar lazy é mais difícil: o erro aparece quando o item é consumido, não quando o generator é criado.

# %% [markdown]
# ## 2. `itertools`: `islice`, `batched`, `groupby` e a pegadinha 🧪
#
# **O que é** — a biblioteca padrão de blocos para montar *pipelines* de iteradores sem materializar nada.
#
# **Por que importa** — três necessidades diárias: amostrar o começo de um fluxo (`islice`), mandar em
# lotes para uma API ou banco (`batched`, Python 3.12+) e agrupar registros consecutivos (`groupby`).
#
# **Como funciona** — todos recebem e devolvem iteradores: dá para encadear sem ocupar memória.

# %%
from collections import Counter
from itertools import batched, chain, groupby, islice

amostra = list(islice(iter_jsonl_gz(ARQ), 5))  # lê só 5 linhas e para
print("islice:", [e["id"] for e in amostra])

tamanhos = [len(lote) for lote in batched(islice(iter_jsonl_gz(ARQ), 2_500), 1_000)]
print("batched (2.500 eventos em lotes de 1.000):", tamanhos)

primeiros_100 = list(islice(iter_jsonl_gz(ARQ), 100))
grupos_sem_ordenar = [(k, len(list(g))) for k, g in groupby(primeiros_100, key=lambda e: e["type"])]
grupos_ordenados = [
    (k, len(list(g)))
    for k, g in groupby(sorted(primeiros_100, key=lambda e: e["type"]), key=lambda e: e["type"])
]
print(f"groupby SEM ordenar: {len(grupos_sem_ordenar)} grupos (tipos repetidos!) "
      f"→ {grupos_sem_ordenar[:4]} ...")
print(f"groupby ordenado   : {len(grupos_ordenados)} grupos → {grupos_ordenados}")
print("Counter (o certo para contar):", Counter(e["type"] for e in primeiros_100).most_common(3))
print("chain:", list(islice(chain("ab", range(3)), 5)))

# %% [markdown]
# A pegadinha: `itertools.groupby` agrupa **elementos consecutivos** com a mesma chave (como o `uniq` do
# Unix), não faz um `GROUP BY` do SQL. Sem ordenar antes, o mesmo tipo aparece em vários grupos.
#
# > 🎤 **Resposta de 30 s:** "`islice` para pegar o começo de um fluxo sem lê-lo todo, `batched` para
# > lotes de N (inserir no banco, chamar API), `groupby` só com entrada ordenada pela chave — ele agrupa
# > vizinhos, não é GROUP BY. Para contar uso `Counter`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `groupby` também reaproveita o iterador interno: se você não consumir o grupo `g` antes de avançar
#   para a próxima chave, ele se esvazia. Sempre `list(g)` ou consuma na hora.
# - Ordenar para agrupar custa O(n log n) e exige tudo na memória. Se a fonte **já vem ordenada** pela
#   chave (ex.: arquivo particionado por data), `groupby` agrupa em O(n) com memória O(1) por grupo.
# - `tee` duplica um iterador, mas guarda em buffer o que um consumidor leu e o outro não: pode virar uma
#   lista disfarçada.
# - Antes do 3.12, `batched` se escrevia com `iter(lambda: list(islice(it, n)), [])`.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Lógica muito encadeada em `itertools` fica ilegível; um `for` explícito às vezes é melhor para o time.
# - Para dados tabulares que cabem na memória, um DataFrame (pandas/Polars) é mais expressivo.

# %% [markdown]
# ## 3. Decorators: o `retry` do projeto e um `timed` com log estruturado 🧪
#
# **O que é** — um *decorator* é uma função que recebe uma função e devolve outra, "embrulhada"
# (`@retry(...)` em cima do `def` é açúcar para `f = retry(...)(f)`).
#
# **Por que importa** — preocupações transversais (*cross-cutting concerns*: retry, tempo, log, cache,
# autenticação) ficam num lugar só, em vez de copiadas em cada função de I/O.
#
# **Como funciona** — `src/oss_lakehouse/utils/retry.py` (usado no download do GH Archive):
#
# ```text
# chamada → tenta f() ── ok ──────────────────────────→ devolve
#                     └─ exceção listada? ─ última? → propaga
#                                          └─ espera uniform(0, min(max, base·2^(n-1))) e tenta de novo
# ```
#
# Três escolhas de sênior no código: (1) só re-tenta as exceções declaradas (erro de programação não deve
# ser re-tentado); (2) *backoff* exponencial com *full jitter* (espera aleatória para N clientes não
# voltarem todos juntos — o *thundering herd*); (3) `sleep` injetável, para o teste não esperar de verdade.

# %%
from oss_lakehouse.utils.retry import retry

esperas: list[float] = []
tentativas = {"n": 0}


@retry(exceptions=(ConnectionError,), attempts=4, base_delay=1.0, sleep=esperas.append)
def baixar_instavel(url: str) -> str:
    tentativas["n"] += 1
    if tentativas["n"] < 3:
        raise ConnectionError(f"falha simulada {tentativas['n']}")
    return f"conteúdo de {url}"


logging.getLogger("oss_lakehouse.utils.retry").setLevel(logging.ERROR)  # silencia os WARN no notebook
print(baixar_instavel("https://data.gharchive.org/2026-10-01-12.json.gz"))
print(f"tentativas: {tentativas['n']} | esperas sorteadas (s): {[round(x, 2) for x in esperas]}")
print("nome preservado por functools.wraps:", baixar_instavel.__name__)


@retry(exceptions=(ConnectionError,), attempts=4, sleep=esperas.append)
def bug_de_programacao() -> None:
    raise KeyError("campo inexistente")  # não é ConnectionError: não deve ser re-tentado


esperas.clear()
try:
    bug_de_programacao()
except KeyError as exc:
    print(f"KeyError propagou na 1ª tentativa (esperas: {len(esperas)}) → {exc!r}")

# %% [markdown]
# Agora um decorator escrito para o projeto: `timed` (`src/oss_lakehouse/utils/timing.py`). Ele mede a
# duração, registra **sucesso ou erro** (o `finally` roda nos dois casos) e manda os campos em `extra=`,
# para o log virar colunas consultáveis (§10). Aceita `@timed` e `@timed(level=...)`.

# %%
import io

from oss_lakehouse.utils.logs import json_logger
from oss_lakehouse.utils.timing import timed

buf = io.StringIO()
log_json = json_logger("demo.timed", stream=buf)


@timed(logger=log_json)
def contar_tipo(path: Path, tipo: str) -> int:
    return sum(1 for e in islice(iter_jsonl_gz(path), 20_000) if e["type"] == tipo)


print("WatchEvent nos 20 mil primeiros:", contar_tipo(ARQ, "WatchEvent"))
print("log gerado:", buf.getvalue().strip())

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Decorator é uma função que embrulha outra. Uso para preocupações
# > transversais: retry com backoff exponencial e jitter nas chamadas de rede, medição de tempo com log
# > estruturado, cache. Sempre com `functools.wraps` para não perder nome e docstring, e com `ParamSpec`
# > para o type checker continuar enxergando a assinatura original."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Decorator com parâmetro** tem 3 níveis: `retry(config) → decorator(func) → wrapper(*args)`.
# - **Idempotência:** retry só é seguro se a operação for idempotente (repetir dá o mesmo efeito).
#   `GET` sim; `POST` que cria pedido não — precisa de chave de idempotência do lado do servidor.
# - **Respeitar o servidor:** em HTTP 429/503, o header `Retry-After` manda mais que o seu backoff
#   (notebook 04). Bibliotecas prontas: `tenacity`, `backoff`, `urllib3.Retry`.
# - **Orçamento total:** 4 tentativas × 30 s de teto pode estourar o SLA do job; em produção se limita
#   também o tempo total (*deadline*).
# - **Ordem de empilhamento:** `@timed @retry def f` mede o tempo com as esperas; `@retry @timed` mede
#   cada tentativa. Os dois fazem sentido, mas medem coisas diferentes.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Decorator escondendo regra de negócio (ex.: muda o resultado) confunde quem lê; deixe para o transversal.
# - Muitos decorators empilhados viram mágica difícil de depurar (o traceback passa por todos os `wrapper`).

# %% [markdown]
# ## 4. Context managers: `@contextmanager`, escrita atômica e `ExitStack` 🧪
#
# **O que é** — um objeto usado com `with` que garante a limpeza (`__exit__`) mesmo se o bloco der erro.
# `contextlib.contextmanager` transforma um generator de um `yield` em context manager.
#
# **Por que importa** — em pipeline, "limpeza garantida" é: fechar arquivo e conexão, liberar *lock*,
# apagar temporário, e **nunca deixar arquivo pela metade** para o próximo job ler.
#
# **Como funciona** — `atomic_write` (`utils/io.py`) grava em `<arquivo>.tmp` e só faz `os.replace` (troca
# atômica no mesmo sistema de arquivos) se o bloco terminar bem. Se der erro, apaga o `.tmp` e propaga.

# %%
from oss_lakehouse.utils.io import atomic_write

destino = DEMO / "contagem_por_tipo.json"
with atomic_write(destino) as f:
    json.dump(Counter(e["type"] for e in islice(iter_jsonl_gz(ARQ), 10_000)), f)
print("gravado:", destino.name, destino.stat().st_size, "bytes")

antes = destino.read_text()
try:
    with atomic_write(destino) as f:
        f.write('{"metade": ')
        raise RuntimeError("job morreu no meio da escrita")
except RuntimeError as exc:
    print("erro:", exc)
print("arquivo antigo intacto:", destino.read_text() == antes,
      "| .tmp sobrou?", (DEMO / "contagem_por_tipo.json.tmp").exists())

# %% [markdown]
# `ExitStack` resolve o caso em que **o número de recursos só é conhecido em tempo de execução** — por
# exemplo, abrir um arquivo de saída por tipo de evento. Todos são fechados na saída, mesmo com erro.

# %%
from contextlib import ExitStack

tipos = ["PushEvent", "CreateEvent", "WatchEvent"]
pasta = DEMO / "por_tipo"
pasta.mkdir(exist_ok=True)
with ExitStack() as stack:
    saidas = {t: stack.enter_context(open(pasta / f"{t}.jsonl", "w")) for t in tipos}
    for e in islice(iter_jsonl_gz(ARQ), 10_000):
        if e["type"] in saidas:
            saidas[e["type"]].write(json.dumps({"id": e["id"], "repo": e["repo"]["name"]}) + "\n")
print("todos fechados:", all(f.closed for f in saidas.values()))
print({p.name: sum(1 for _ in p.open()) for p in sorted(pasta.glob("*.jsonl"))})

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Context manager garante a limpeza com `with`, mesmo com exceção. Escrevo
# > com `@contextmanager` quando é simples: código antes do `yield` é o `__enter__`, depois é o
# > `__exit__`, e o `try/finally` cuida do erro. Exemplo real: escrita atômica — grava em temporário e
# > renomeia; o consumidor nunca lê arquivo pela metade. Para N recursos dinâmicos, `ExitStack`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `__exit__` que devolve `True` **engole** a exceção — quase sempre é bug.
# - `os.replace` é atômico só **no mesmo sistema de arquivos**. Em object storage (ADLS, S3) não existe
#   rename atômico de verdade: por isso o Delta Lake usa o *transaction log* — o arquivo Parquet pode estar
#   lá, mas só "existe" para o leitor depois do commit no `_delta_log` (notebook 10).
# - Versões assíncronas: `async with` e `contextlib.asynccontextmanager` (conexões de banco assíncronas).
# - `contextlib.suppress(FileNotFoundError)` substitui `try/except: pass` com intenção explícita.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Objeto que vive o processo todo (pool de conexões de uma API) não cabe num `with` curto; use o ciclo de
#   vida do framework.
# - Num lakehouse, escrita atômica de tabela é responsabilidade do formato (Delta), não sua.

# %% [markdown]
# ## 5. Typing moderno: generics PEP 695, `Protocol`, `TypedDict`, `Literal`, `ParamSpec` 🧪
#
# **O que é** — anotações de tipo que um verificador estático (*type checker*: mypy, pyright) usa para
# achar bug **antes de rodar**. O Python não as impõe em tempo de execução.
#
# **Por que importa** — em pipeline, o erro caro é o que aparece às 3h da manhã com dado de produção.
# Tipos são documentação que o computador confere: assinatura de função vira contrato.
#
# **Como funciona** — o que cada um resolve:
#
# | Recurso | Para quê | Exemplo do projeto |
# |---|---|---|
# | Generics PEP 695 (3.12) | função/classe que preserva o tipo do item | `def merge_intervals[T: SupportsLessThan](...)` |
# | `Protocol` | "qualquer coisa que tenha estes métodos" (*duck typing* checado) | um `Sink` com `write(batch)` |
# | `TypedDict` | formato de um dict (JSON) sem custo de runtime | o evento cru do GitHub |
# | `Literal` | conjunto fechado de valores | `on_error: Literal["raise", "skip"]` |
# | `ParamSpec` | decorator que preserva a assinatura | `retry`, `timed` |

# %%
from collections.abc import Iterable, Iterator
from typing import Literal, NotRequired, Protocol, TypedDict, get_type_hints, runtime_checkable


class ActorDict(TypedDict):
    id: int
    login: str


class GhEventDict(TypedDict):
    id: str
    type: str
    actor: ActorDict
    created_at: str
    org: NotRequired[dict]  # ausente em repo pessoal


@runtime_checkable
class Sink(Protocol):
    def write(self, batch: list[GhEventDict]) -> int: ...


class ListaSink:  # não herda de Sink: basta ter o método certo (tipagem estrutural)
    def __init__(self) -> None:
        self.linhas: list[GhEventDict] = []

    def write(self, batch: list[GhEventDict]) -> int:
        self.linhas.extend(batch)
        return len(batch)


def em_lotes[T](itens: Iterable[T], n: int) -> Iterator[list[T]]:  # PEP 695: T declarado na função
    for lote in batched(itens, n):
        yield list(lote)


type Modo = Literal["append", "overwrite"]  # alias de tipo PEP 695


def carregar(fonte: Iterable[GhEventDict], sink: Sink, modo: Modo = "append") -> int:
    return sum(sink.write(lote) for lote in em_lotes(fonte, 1_000))


destino_lista = ListaSink()
n = carregar(islice(iter_jsonl_gz(ARQ), 3_000), destino_lista)  # type: ignore[arg-type]
print("carregados:", n, "| ListaSink é um Sink?", isinstance(destino_lista, Sink))
print("TypedDict em runtime é só dict:", type(destino_lista.linhas[0]).__name__)
print("hints de carregar():", {k: str(v) for k, v in get_type_hints(carregar).items()})

# %% [markdown]
# Prova de que o Python **não** impõe tipo em runtime — quem pega o erro é o type checker no CI:

# %%
print("carregar() com modo inválido rodou mesmo assim:", carregar([], destino_lista, modo="truncate"))  # type: ignore[arg-type]

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Uso tipos como contrato verificado no CI. Generics PEP 695 para funções que
# > preservam o tipo do item; `Protocol` para depender de comportamento e não de herança — facilita teste
# > com um fake; `TypedDict` para descrever JSON sem custo; `Literal` para opções fechadas; `ParamSpec` em
# > decorators. Mas tipo não valida dado em runtime: na fronteira com o mundo externo, uso pydantic."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Variância:** `list[int]` não é `list[float]` (lista é mutável → invariante); `Sequence[int]` é
#   `Sequence[float]` (covariante). Por isso parâmetros devem pedir o tipo mais abstrato (`Iterable`,
#   `Sequence`, `Mapping`) e retornos o mais concreto.
# - **`from __future__ import annotations`** adia a avaliação das anotações (string); no 3.14 a avaliação
#   adiada virou padrão (PEP 649). Bibliotecas que leem anotações em runtime (pydantic) lidam com isso.
# - **`typing.TypeIs`/`TypeGuard`**: funções de checagem que estreitam o tipo (`if is_push(e): ...`).
# - **Neste repositório** o ruff roda no CI; um type checker (mypy ou pyright) **não** está nas
#   dependências — seria o próximo passo natural (`uv add --dev mypy`).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Anotação que mente (`-> int` que às vezes devolve `None`) é pior que nenhuma.
# - `Any` em toda parte anula o benefício; prefira `object` + estreitamento.
# - Tipar código de notebook exploratório é exagero; tipar o pacote não é.

# %% [markdown]
# ## 6. `dataclasses` (`frozen`, `slots`) × pydantic 🧪
#
# **O que é** — `dataclass` gera `__init__`, `__repr__`, `__eq__` a partir das anotações: estrutura de
# dados **interna**, sem validação. Pydantic **valida e converte** (*parse, don't validate*): recebe dado
# externo (JSON, API, arquivo) e devolve objeto tipado ou um erro detalhado.
#
# **Por que importa** — a fronteira do pipeline (o que vem de fora) precisa de validação; o miolo
# (objetos que você mesmo criou) precisa de leveza. Usar a ferramenta errada custa CPU ou custa bug.
#
# **Como funciona** — `frozen=True` impede mutação (seguro como chave de dict e entre threads);
# `slots=True` troca o `__dict__` por campos fixos (menos memória por objeto, atributo novo vira erro).

# %%
import dataclasses
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RepoRef:
    id: int
    name: str


@dataclass
class RepoRefSemSlots:
    id: int
    name: str


r = RepoRef(1, "apache/spark")
try:
    r.name = "outro"  # type: ignore[misc]
except dataclasses.FrozenInstanceError as exc:
    print("frozen:", type(exc).__name__, "-", exc)
print("hashable (vira chave de dict/set):", {r: "ok"}[RepoRef(1, "apache/spark")])

repos = [(e["repo"]["id"], e["repo"]["name"]) for e in islice(iter_jsonl_gz(ARQ), 50_000)]
_, mb_slots, _ = pico_mb(lambda: [RepoRef(i, nm) for i, nm in repos])
_, mb_dict, _ = pico_mb(lambda: [RepoRefSemSlots(i, nm) for i, nm in repos])
print(f"50 mil objetos — com slots: {mb_slots:.1f} MB | sem slots: {mb_dict:.1f} MB "
      f"({mb_dict / mb_slots:.1f}×)")

# %% [markdown]
# Agora pydantic validando o **evento real do GitHub**. O modelo declara o que o pipeline exige; o resto
# do JSON é ignorado (`extra="ignore"`). `type` é um `Literal` com os 16 tipos documentados.

# %%
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationError

TipoEvento = Literal[
    "PushEvent", "CreateEvent", "DeleteEvent", "PullRequestEvent", "IssueCommentEvent", "IssuesEvent",
    "PullRequestReviewEvent", "WatchEvent", "PullRequestReviewCommentEvent", "ReleaseEvent", "ForkEvent",
    "MemberEvent", "CommitCommentEvent", "GollumEvent", "PublicEvent", "DiscussionEvent",
]


class Actor(BaseModel):
    id: int
    login: str = Field(min_length=1)


class Repo(BaseModel):
    id: int
    name: str = Field(pattern=r"^[^/]+/[^/]+$")  # dono/nome


class GhEvent(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    id: str
    type: TipoEvento
    actor: Actor
    repo: Repo
    created_at: datetime  # string ISO-8601 → datetime com fuso
    public: bool
    payload: dict


ev = GhEvent.model_validate(amostra[0])
print(ev.type, ev.actor.login, ev.repo.name, repr(ev.created_at))

ruim = {**amostra[0], "type": "PushEvnt", "created_at": "ontem", "repo": {"id": "abc", "name": "sem-barra"}}
try:
    GhEvent.model_validate(ruim)
except ValidationError as exc:
    print(f"\n{exc.error_count()} erros, todos de uma vez:")
    for err in exc.errors():
        print(f"  {'.'.join(map(str, err['loc'])):12} {err['type']:22} {err['msg'][:70]}")

# %% [markdown]
# O modelo aplicado às 20 mil primeiras linhas do arquivo real — e o custo, comparado à dataclass:

# %%
lote = list(islice(iter_jsonl_gz(ARQ), 20_000))

t0 = time.perf_counter()
validos, invalidos = [], []
for bruto in lote:
    try:
        validos.append(GhEvent.model_validate(bruto))
    except ValidationError as exc:
        invalidos.append((bruto["id"], exc.errors()[0]["type"]))
t_pyd = time.perf_counter() - t0

t0 = time.perf_counter()
_ = [RepoRef(b["repo"]["id"], b["repo"]["name"]) for b in lote]
t_dc = time.perf_counter() - t0

print(f"válidos: {len(validos):,} | inválidos: {len(invalidos)} {invalidos[:3]}")
print(f"pydantic: {t_pyd * 1e6 / len(lote):.1f} µs/evento | dataclass (sem validar): "
      f"{t_dc * 1e6 / len(lote):.2f} µs/evento")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Dataclass para estrutura interna — `frozen` para imutável e hashable,
# > `slots` para economizar memória. Pydantic na fronteira: valida e converte dado externo e devolve
# > **todos** os erros com o caminho do campo. Medi aqui: pydantic custa microssegundos por evento — ok para
# > configuração, API e lotes pequenos; para milhões de linhas, a validação vai para o Spark (schema +
# > expectations), não para um loop Python."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Pydantic v2** tem o núcleo em Rust (`pydantic-core`); o v1 era Python puro e bem mais lento.
# - **Modo estrito** (`strict=True`) desliga coerção: `"123"` deixa de virar `123`. Em contrato de dados,
#   coerção silenciosa esconde fonte mudando de tipo.
# - **`model_json_schema()`** gera JSON Schema: dá para publicar o contrato da API/evento (notebook 08).
# - **Onde já usamos:** `config.py` é um `pydantic-settings` — lê `OSSLH_*` do ambiente com tipo validado.
# - **Alternativas:** `attrs` (dataclass com validadores), `msgspec` (decodifica JSON direto para struct
#   validada, muito rápido).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Pydantic linha a linha em volume de big data: use schema do Spark + regras de qualidade.
# - `slots=True` impede atributo dinâmico e complica herança múltipla.
# - `frozen` não é imutabilidade profunda: um campo `list` continua mutável por dentro.

# %% [markdown]
# ## 7. `functools`: `lru_cache`, `partial`, `singledispatch` 🧪
#
# **O que é** — utilitários para funções: memorizar resultado (*memoization*), fixar argumentos e
# despachar por tipo do 1º argumento.
#
# **Por que importa** — `lru_cache` evita recalcular o caro e repetido (enriquecimento por chave com poucos
# valores distintos); `partial` configura uma função uma vez e passa adiante (para `map`, executor, Spark);
# `singledispatch` substitui `if isinstance(...) elif ...` gigante.
#
# **Como funciona** — `lru_cache(maxsize=N)` guarda os N resultados mais recentes num dict indexado pelos
# argumentos (precisam ser hashable).

# %%
from functools import lru_cache, partial, singledispatch


@lru_cache(maxsize=4_096)
def classificar_ator(login: str) -> str:
    """Simula um enriquecimento caro (consulta a cadastro, regex pesada, chamada de API)."""
    time.sleep(0.0005)
    return "bot" if login.endswith("[bot]") else "humano"


logins = [e["actor"]["login"] for e in islice(iter_jsonl_gz(ARQ), 5_000)]
t0 = time.perf_counter()
classes = Counter(classificar_ator(lg) for lg in logins)
t_cache = time.perf_counter() - t0
info = classificar_ator.cache_info()
print(classes, f"| {t_cache:.2f}s com cache")
print(f"cache: {info.hits:,} acertos, {info.misses:,} faltas → só {info.misses:,} chamadas reais "
      f"para {len(logins):,} eventos ({info.hits / len(logins):.0%} evitadas)")

so_push = partial(contar_tipo, tipo="PushEvent")  # configura uma vez, usa em qualquer lugar
print("partial:", so_push.func.__name__, so_push.keywords)


@singledispatch
def para_json(valor: object) -> object:
    raise TypeError(f"sem serializador para {type(valor).__name__}")


@para_json.register
def _(valor: datetime) -> str:
    return valor.isoformat()


@para_json.register
def _(valor: set) -> list:
    return sorted(valor)


@para_json.register
def _(valor: RepoRef) -> dict:
    return dataclasses.asdict(valor)


doc = {"quando": ev.created_at, "tipos": {"WatchEvent", "ForkEvent"}, "repo": r}
print(json.dumps(doc, default=para_json))

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "`lru_cache` para memorizar função pura com argumentos hashable — mostro o
# > `cache_info()` para provar a taxa de acerto; `partial` para fixar argumentos e passar a função para um
# > executor; `singledispatch` para polimorfismo por tipo sem cadeia de `isinstance`, como serializar
# > tipos que o `json` não conhece."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `lru_cache` em **método** segura `self` vivo para sempre (vazamento de memória); use `cached_property`
#   ou cache no nível do módulo.
# - O cache é **por processo**: no Spark, cada executor (e cada worker Python) tem o seu. Para um
#   enriquecimento por chave no Spark, o certo é um *broadcast join* com a tabela de referência.
# - Função com efeito colateral ou dependente de tempo não pode ser memorizada.
# - `functools.cache` = `lru_cache(maxsize=None)`: cresce sem limite.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Cardinalidade alta (cada chave aparece uma vez): o cache só gasta memória.
# - `singledispatch` despacha só pelo 1º argumento; para método use `singledispatchmethod`.

# %% [markdown]
# ## 8. Concorrência: threads × processos × asyncio — escolhido pelo gargalo 🧪
#
# **O que é** — três formas de fazer várias coisas "ao mesmo tempo":
# - **threads** (`ThreadPoolExecutor`): várias linhas de execução no mesmo processo, memória compartilhada;
# - **processos** (`ProcessPoolExecutor`): vários interpretadores Python, cada um com sua memória;
# - **asyncio**: uma thread só, que alterna entre tarefas enquanto elas **esperam** I/O.
#
# **Por que importa** — o GIL (*Global Interpreter Lock*, §9) deixa só uma thread executar bytecode Python
# por vez. Então: espera de rede/disco → threads ou asyncio; CPU em Python puro → processos. Errar a
# escolha dá paralelismo nenhum.
#
# **Como funciona** — dois experimentos com 4 arquivos reais do GH Archive (4 horas, ~75 MB):
# 1. **parse de JSON** (CPU em Python puro, segura o GIL) — `count_by_field`;
# 2. **só descomprimir** (o `zlib` é C e **solta o GIL** enquanto descomprime) — `count_lines_gz`.

# %%
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from oss_lakehouse.utils.io import count_by_field, count_lines_gz

ARQS = sorted(CACHE.glob("2026-10-01-1[0-1].json.gz")) + sorted(CACHE.glob("2026-10-01-[8-9].json.gz"))
print([a.name for a in ARQS], f"| {sum(a.stat().st_size for a in ARQS) / 1e6:.0f} MB | CPUs: {os.cpu_count()}")


def medir(nome, fn):
    t0 = time.perf_counter()
    out = fn()
    return nome, out, time.perf_counter() - t0


def com_pool(executor_cls, func):
    with executor_cls(max_workers=4) as pool:  # o with espera e encerra os workers
        return list(pool.map(func, ARQS))


resultados = {}
for tarefa, func in [("parse JSON (CPU, segura o GIL)", count_by_field), ("descomprimir (C, solta o GIL)", count_lines_gz)]:
    linhas = [
        medir("sequencial", lambda: [func(a) for a in ARQS]),
        medir("threads x4", lambda: com_pool(ThreadPoolExecutor, func)),
        medir("processos x4", lambda: com_pool(ProcessPoolExecutor, func)),
    ]
    base = linhas[0][2]
    print(f"\n{tarefa}")
    for nome, out, secs in linhas:
        print(f"  {nome:13} {secs:6.2f}s  speedup {base / secs:4.1f}×")
    assert linhas[0][1] == linhas[1][1] == linhas[2][1]  # mesmo resultado nos 3 modos
    resultados[tarefa] = {nome: secs for nome, _, secs in linhas}

# %% [markdown]
# Leitura do resultado (os números exatos variam com a carga da máquina, a direção não):
# - **parse JSON**: threads não aceleram — tendem a até piorar, pela disputa do GIL entre as 4 threads (o
#   `json.loads` e o loop são bytecode Python); processos aceleram.
# - **descomprimir**: threads já ganham, porque o `zlib` solta o GIL; processos também, pagando o custo de
#   criar processos.
#
# **asyncio** brilha quando o gargalo é **esperar** (rede). Sem internet aqui, a latência de rede é
# simulada com `asyncio.sleep` — o que se mede é o padrão de espera, não um download real:

# %%
import asyncio

N_REQ, LATENCIA = 20, 0.1


async def baixar_hora(hora: int) -> str:
    await asyncio.sleep(LATENCIA)  # no código real: await client.get(url) (httpx/aiohttp)
    return f"2026-10-01-{hora}"


def baixar_hora_bloqueante(hora: int) -> str:
    time.sleep(LATENCIA)
    return f"2026-10-01-{hora}"


t0 = time.perf_counter()
seq = [baixar_hora_bloqueante(h) for h in range(N_REQ)]
t_seq = time.perf_counter() - t0

t0 = time.perf_counter()
with ThreadPoolExecutor(max_workers=N_REQ) as pool:
    thr = list(pool.map(baixar_hora_bloqueante, range(N_REQ)))
t_thr = time.perf_counter() - t0

limite = asyncio.Semaphore(10)  # no máximo 10 requisições simultâneas: respeita o rate limit da fonte


async def baixar_com_limite(h: int) -> str:
    async with limite:
        return await baixar_hora(h)


t0 = time.perf_counter()
asy = await asyncio.gather(*(baixar_com_limite(h) for h in range(N_REQ)))  # noqa: F704 (top-level await do Jupyter)
t_asy = time.perf_counter() - t0

assert seq == thr == list(asy)
print(f"{N_REQ} 'downloads' de {LATENCIA}s: sequencial {t_seq:.2f}s | threads {t_thr:.2f}s | "
      f"asyncio (máx. 10 simultâneos) {t_asy:.2f}s")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Escolho pelo gargalo. Esperar rede ou disco: threads, ou asyncio quando são
# > milhares de conexões. CPU em Python puro: processos, porque o GIL serializa threads. Medi aqui com 4
# > horas do GH Archive: o parse de JSON com threads não acelerou — piorou — e com processos acelerou; só descomprimir,
# > que é C e solta o GIL, ganhou com threads. E em volume de verdade, quem paraleliza é o Spark — não
# > escrevo pool de processos para processar terabyte."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Custo do processo:** cada worker é um interpretador; argumentos e resultados viajam serializados
#   (*pickle*). Devolver o resultado agregado (um dict de contagens) e não as linhas é o que torna o ganho
#   possível — devolver 92 mil dicts por arquivo mataria o ganho na serialização.
# - **`fork` × `spawn` × `forkserver`:** Linux usava `fork` (copia o processo — rápido, mas perigoso com
#   threads/JVM vivas); macOS/Windows usam `spawn`; o Python 3.14 mudou o padrão do Linux para
#   `forkserver`. Com `spawn`/`forkserver` a função precisa ser importável — por isso `count_by_field`
#   está no pacote e não no notebook.
# - **asyncio + código bloqueante:** uma chamada síncrona (`requests.get`, `time.sleep`) dentro de uma
#   corrotina trava **todas** as tarefas. Use biblioteca assíncrona ou `asyncio.to_thread`.
# - **Controle de concorrência:** `Semaphore` limita requisições simultâneas — paralelismo sem limite
#   vira 429 da API (notebook 04).
# - **Exceções:** `pool.map` re-levanta a primeira exceção ao iterar; `asyncio.gather(...,
#   return_exceptions=True)` ou `TaskGroup` (3.11+) para coletar/cancelar.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Threads com estado compartilhado mutável exigem lock — prefira cada tarefa devolver seu resultado.
# - Processos para tarefas de milissegundos: o custo de criar e serializar supera o ganho.
# - asyncio contamina a base (toda a cadeia vira `async`); para 20 downloads, `ThreadPoolExecutor` é mais simples.

# %% [markdown]
# ## 9. GIL e o Python *free-threaded* (3.13+) — com cautela 🧪
#
# **O que é** — o GIL é um *lock* global do CPython: só uma thread executa bytecode Python por vez. Ele
# simplifica o gerenciamento de memória (contagem de referências) e a vida de extensões em C. A PEP 703
# criou um build opcional **sem GIL** (*free-threaded*, executável `python3.13t`), experimental no 3.13 e
# oficialmente suportado (ainda opcional) no 3.14.
#
# **Por que importa** — sem GIL, threads poderiam usar vários núcleos em código Python puro — o
# experimento da §8 mudaria de resultado. Mas o ecossistema precisa acompanhar.
#
# **Como funciona** — dá para verificar em runtime qual build está rodando:

# %%
import sysconfig

print("versão:", sys.version.split()[0])
print("build free-threaded (Py_GIL_DISABLED):", bool(sysconfig.get_config_var("Py_GIL_DISABLED")))
print("GIL ligado agora:", sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else "sim (API só existe no 3.13+)")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "O GIL faz threads não paralelizarem CPU em Python puro; I/O e extensões em C
# > que soltam o GIL — zlib, NumPy, Arrow — paralelizam. O build free-threaded existe desde o 3.13 e é
# > suportado no 3.14, mas é opcional, tem custo em código single-thread e depende de cada extensão em C
# > declarar suporte. Eu não basearia um pipeline de produção nele hoje; meço antes. E no Spark o GIL
# > quase não importa: o trabalho pesado roda na JVM e o paralelismo é entre processos/executores."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Sem GIL, código que "funcionava por acaso" (estado compartilhado sem lock) passa a ter *race condition*.
# - Extensões em C antigas reativam o GIL ao serem importadas no build free-threaded (com aviso).
# - Outra frente: subinterpretadores (PEP 734, módulo `concurrent.interpreters` no 3.14) — vários
#   interpretadores no mesmo processo, cada um com seu GIL.
# - Este ambiente roda 3.12 padrão (saída acima): nada aqui foi medido sem GIL.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Não troque processos por threads "porque o GIL acabou" sem medir com as suas dependências reais.

# %% [markdown]
# ## 10. Tratamento de erro e log estruturado 🧪
#
# **O que é** — três práticas: (1) **hierarquia de exceções** do domínio (o chamador decide o que fazer
# por tipo); (2) **encadeamento** (`raise NovoErro(...) from causa`) para não perder a origem;
# (3) **log estruturado** — JSON por linha, com campos (`run_id`, `arquivo`, `linhas`) em vez de texto livre.
#
# **Por que importa** — em pipeline há duas classes de erro: **do dado** (uma linha ruim: isola, conta,
# segue) e **do sistema** (credencial, rede, disco: falha alto e re-tenta). Tratar as duas igual é o bug
# clássico — ou o job morre por uma linha, ou engole uma pane inteira.
#
# **Como funciona** — `iter_jsonl_gz` levanta `BadLineError` (com arquivo e linha) e preserva a causa via
# `from`; com `on_error="skip"` isola a linha e anota onde ela estava (o embrião de uma quarentena).
# `except*` (3.11+) trata grupos de erros de um lote inteiro de uma vez.

# %%
from oss_lakehouse.utils.io import BadLineError

corrompido = DEMO / "corrompido.json.gz"
with gzip.open(ARQ, "rt") as src, gzip.open(corrompido, "wt") as dst:
    for i, linha in enumerate(islice(src, 1_000), start=1):
        dst.write(linha[:40] + "\n" if i in (17, 503) else linha)  # 2 linhas truncadas de propósito

try:
    sum(1 for _ in iter_jsonl_gz(corrompido))
except BadLineError as exc:
    print("falha alto:", exc.path.split("/")[-1], "linha", exc.line_no, "| causa:", type(exc.__cause__).__name__)

ruins: list[int] = []
logging.getLogger("oss_lakehouse.utils.io").setLevel(logging.ERROR)
ok = sum(1 for _ in iter_jsonl_gz(corrompido, on_error="skip", bad_lines=ruins))
print(f"modo skip: {ok} linhas boas, linhas ruins isoladas: {ruins}")

# %%
erros = []
for bruto in [amostra[1], {**amostra[2], "type": "X"}, {**amostra[3], "actor": {}}]:
    try:
        GhEvent.model_validate(bruto)
    except ValidationError as exc:
        erros.append(exc)

try:
    if erros:
        raise ExceptionGroup("lote com eventos inválidos", erros)
except* ValidationError as grupo:
    print(f"except*: {len(grupo.exceptions)} eventos inválidos tratados juntos no fim do lote")

buf = io.StringIO()
plog = json_logger("pipeline.bronze", stream=buf)
plog.info("lote carregado", extra={"run_id": "2026-10-01T12", "arquivo": corrompido.name,
                                    "linhas_ok": ok, "linhas_ruins": len(ruins)})
try:
    1 / 0
except ZeroDivisionError:
    plog.exception("falha inesperada", extra={"run_id": "2026-10-01T12"})
for linha in buf.getvalue().splitlines():
    d = json.loads(linha)
    d.pop("exc", None)
    print(d)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Separo erro de dado de erro de sistema. Linha ruim vai para quarentena com o
# > motivo e o job segue — com um limite: se passar de X%, falha. Erro de sistema falha alto, com retry só
# > no que é transitório. Sempre `raise ... from` para não perder a causa, nunca `except: pass`. Log em JSON
# > com `run_id`, para correlacionar no Log Analytics e montar alerta por campo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Limite de erros:** quarentena sem limite esconde uma fonte que quebrou inteira. Regra comum: falhar
#   se a taxa de rejeição passar de um limiar (notebook 08).
# - **No Spark:** `mode=PERMISSIVE` + `_corrupt_record` é o equivalente (a bronze usa); no Databricks o
#   Auto Loader tem `_rescued_data` para colunas que não casam com o schema.
# - **`logger.exception`** só dentro de `except` (anexa o traceback). Não use f-string no log:
#   `log.info("x %s", v)` só formata se o nível estiver ativo.
# - **Nunca logar dado pessoal** (e-mail, CPF, token) — LGPD (notebook 11).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `try/except` em volta de cada linha num loop de milhões custa CPU; valide em lote quando der.
# - JSON é ruim de ler no terminal local; um formatter por ambiente (texto em dev, JSON em produção) resolve.

# %% [markdown]
# ## 11. Empacotamento: `pyproject.toml`, uv e wheel 🧪
#
# **O que é** — `pyproject.toml` (PEP 621) declara nome, versão, dependências e *build backend*. O **uv**
# resolve e instala dependências e grava o `uv.lock` (versões exatas, reproduzíveis). A **wheel** (`.whl`)
# é o pacote pronto para instalar — é o que vai para o cluster Databricks.
#
# **Por que importa** — notebook com lógica copiada não é testável nem versionável. O padrão sênior é:
# lógica no pacote → teste → wheel → instalada no job (via Asset Bundle, notebook 13).
#
# **Como funciona** — gerando a wheel deste repositório, offline, e olhando dentro dela:

# %%
import shutil
import subprocess
import zipfile

from oss_lakehouse.config import PROJECT_ROOT as ROOT

dist = DEMO / "dist"
shutil.rmtree(dist, ignore_errors=True)
uv = shutil.which("uv") or str(Path.home() / ".local" / "bin" / "uv")
proc = subprocess.run([uv, "build", "--wheel", "--offline", "--out-dir", str(dist)], cwd=ROOT,
                      capture_output=True, text=True,
                      env={**{k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}, "NO_COLOR": "1"})
print(proc.stderr.strip().splitlines()[-1])
whl = next(dist.glob("*.whl"))
with zipfile.ZipFile(whl) as z:
    nomes = z.namelist()
    meta = z.read(next(n for n in nomes if n.endswith("METADATA"))).decode()
print(whl.name, f"{whl.stat().st_size / 1e3:.0f} KB,", len(nomes), "arquivos")
print("módulos:", sorted(n for n in nomes if n.endswith(".py"))[:8], "...")
print("dependências declaradas:", [ln.split(": ")[1] for ln in meta.splitlines() if ln.startswith("Requires-Dist")][:7])
print(subprocess.run(["grep", "-A3", "^\\[build-system\\]", "pyproject.toml"], cwd=ROOT, capture_output=True, text=True).stdout)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Projeto Python com `pyproject.toml` e layout `src/`; uv para dependências com
# > lockfile — o CI instala exatamente o que testei. A lógica vira wheel, versionada, e o job no Databricks
# > instala a wheel; o notebook só orquestra. Dependências de dev (pytest, ruff) ficam num grupo separado e
# > não vão para a wheel."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Layout `src/`** impede importar o pacote "sem querer" do diretório corrente: o teste roda contra o
#   pacote instalado, como em produção.
# - **Lock × range:** biblioteca declara faixas (`pydantic>=2.13`); aplicação/job trava versões (lock).
#   Aqui `pyspark==4.2.0` e `delta-spark==4.4.0` estão travados porque o JAR do Delta tem de casar com o Spark.
# - **No Databricks** o runtime já traz PySpark: a wheel do job não deve reinstalar outra versão.
# - **Versionamento:** SemVer (MAJOR.MINOR.PATCH) e tag Git por release (notebook 13).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Script de uso único não precisa virar pacote.
# - Wheel com dependência nativa (compilada) precisa ser gerada para a plataforma do cluster (Linux x86_64).

# %% [markdown]
# ## 12. pytest: fixtures, `parametrize`, `monkeypatch`, `tmp_path` 🧪
#
# **O que é** — o framework de testes padrão do mercado Python. Quatro recursos cobrem 90% dos testes de
# pipeline:
#
# | Recurso | Para quê | Em `tests/test_utils_io.py` |
# |---|---|---|
# | fixture | preparar dado/recurso reutilizável | `jsonl_gz`: arquivo .gz com 1 linha corrompida |
# | `tmp_path` | pasta temporária única por teste | todos os testes de arquivo |
# | `parametrize` | mesmo teste, vários casos | `test_many_encadeia` (1, 2, 3 cópias) |
# | `monkeypatch` | trocar uma função/variável só durante o teste | simular `os.replace` falhando ("disco cheio") |
#
# **Por que importa** — teste de pipeline é sobre os casos ruins: linha corrompida, rename que falha,
# arquivo vazio. Sem fixture e monkeypatch, esses casos não se testam.
#
# **Como funciona** — trecho real do teste de escrita atômica com falha simulada:
#
# ```python
# def test_atomic_write_rename_falha(tmp_path, monkeypatch):
#     monkeypatch.setattr(uio.os, "replace", boom)        # os.replace agora levanta OSError
#     with pytest.raises(OSError, match="disco cheio"), atomic_write(tmp_path / "out.txt") as f:
#         f.write("x")
#     assert not (tmp_path / "out.txt.tmp").exists()      # não sobrou lixo
# ```
#
# Rodando a suíte dos utilitários (sem Spark):

# %%
res = subprocess.run([sys.executable, "-m", "pytest", "-q", "--color=no", "-p", "no:cacheprovider",
                      "tests/test_utils_io.py"], cwd=ROOT, capture_output=True, text=True)
print(res.stdout.strip().splitlines()[-1])
res = subprocess.run([sys.executable, "-m", "pytest", "-q", "--collect-only", "-p", "no:cacheprovider",
                      "tests/test_utils_io.py"], cwd=ROOT, capture_output=True, text=True)
print("\n".join(ln for ln in res.stdout.splitlines() if "parametriz" in ln or "many" in ln)[:400])

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Testo a lógica no pacote com pytest: fixture para o dado de entrada,
# > `tmp_path` para arquivo, `parametrize` para os casos de borda, `monkeypatch` para simular falha de I/O
# > sem infraestrutura. Para Spark, uma `SparkSession` com escopo de sessão (sobe a JVM uma vez) e DataFrames
# > pequenos criados no teste. O teste roda no pre-commit/CI antes do deploy."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Escopo de fixture:** `function` (padrão), `module`, `session`. A sessão Spark em `tests/conftest.py`
#   é `scope="session"` — subir a JVM custa segundos.
# - **`caplog`** captura logs para assertar (usado nos testes do `timed`).
# - **Pirâmide:** muitos testes unitários de função pura, alguns de integração (Spark local + Delta em
#   `tmp_path`, como `test_bronze_e_idempotente`), poucos de ponta a ponta no workspace de dev.
# - **Comparar DataFrames:** `chispa.assert_df_equality` (nas dependências) ou
#   `pyspark.testing.assertDataFrameEqual` (Spark 3.5+).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `monkeypatch` demais acopla o teste à implementação; prefira injetar a dependência (como o `sleep` do `retry`).
# - Teste de Spark é lento: não teste a biblioteca (o Spark já é testado), teste a **sua** regra.

# %% [markdown]
# ## 13. pandas × Polars × PySpark — critérios, não religião 🧪
#
# **O que é** — três motores de DataFrame: **pandas** (1 núcleo, tudo na memória, ecossistema enorme),
# **Polars** (Rust, multinúcleo, avaliação preguiçosa e *streaming*, uma máquina), **PySpark** (distribuído,
# vários nós, overhead fixo de JVM e de agendamento).
#
# **Por que importa** — usar Spark para 50 MB paga segundos de overhead; usar pandas para 500 GB não
# termina. A pergunta de entrevista é "qual você usaria e por quê".
#
# **Como funciona** — critérios:
#
# | Critério | pandas | Polars | PySpark |
# |---|---|---|---|
# | Volume confortável | até ~alguns GB (cabe 5–10× na RAM) | dezenas de GB numa máquina | sem teto prático (cluster) |
# | Paralelismo | 1 núcleo | todos os núcleos | todos os nós |
# | Execução | imediata (*eager*) | lazy com otimizador | lazy com otimizador (Catalyst) |
# | Overhead para começar | ~0 | ~0 | segundos (JVM, plano, tarefas) |
# | No Databricks | ok no driver, não distribui | ok no driver, não distribui | o motor nativo (Photon, Delta, UC) |
#
# Neste ambiente **pandas e Polars não estão instalados** (não são dependência do projeto) — a medição
# abaixo compara o extremo "Python puro numa máquina" × Spark local, na mesma contagem por tipo de evento
# de 1 arquivo de 1 hora. Agora sim subimos o Spark.

# %%
from oss_lakehouse.spark import get_spark
from pyspark.sql import functions as F

t0 = time.perf_counter()
spark = get_spark("02")
t_boot = time.perf_counter() - t0

t0 = time.perf_counter()
py_counts = count_by_field(ARQ)
t_py = time.perf_counter() - t0

t0 = time.perf_counter()
eventos_sp = spark.read.schema("type string").json(str(ARQ))  # schema explícito: só a coluna usada
sp_counts = {r["type"]: r["n"] for r in eventos_sp.groupBy("type").agg(F.count("*").alias("n")).collect()}
t_sp = time.perf_counter() - t0
assert py_counts == sp_counts

import importlib.util
print({lib: importlib.util.find_spec(lib) is not None for lib in ["pandas", "polars", "pyarrow"]})
print(f"1 arquivo ({sum(py_counts.values()):,} eventos): Python puro {t_py:.1f}s | "
      f"Spark local {t_sp:.1f}s (+{t_boot:.1f}s para subir a sessão)")

# %% [markdown]
# Para **1 arquivo** o Python puro é competitivo, mesmo com o Spark lendo só a coluna `type` (schema
# explícito; sem ele, `spark.read.json` leria o arquivo duas vezes para inferir o schema). A vantagem do
# Spark aparece quando o volume passa do que uma máquina processa no tempo exigido, e quando a saída é uma
# tabela Delta governada. O `.gz` também não é divisível (*splittable*): 1 arquivo = 1 tarefa, então o
# Spark local não paraleliza a leitura de um único arquivo.
#
# > 🎤 **Resposta de 30 s:** "Escolho por volume, latência e onde o dado mora. Dado que cabe numa máquina:
# > Polars (ou pandas, se o time e o ecossistema pedem). Dado que não cabe, ou que já está no lakehouse com
# > governança: Spark. No Databricks, o padrão é Spark para o pipeline e pandas/Polars só no driver para
# > coisa pequena — `toPandas()` em tabela grande derruba o driver."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **pandas API on Spark** (`import pyspark.pandas as ps`): sintaxe pandas, execução distribuída — útil para
#   portar código, com diferenças sutis (ordem não garantida, índice custa caro).
# - **DuckDB** é a 4ª opção: SQL analítico local muito rápido sobre Parquet/Delta.
# - **Arrow** é o formato de memória colunar comum: pandas 2 (backend Arrow), Polars e Spark (`toPandas`,
#   pandas UDF) trocam dados por Arrow sem converter linha a linha.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Spark para arquivo pequeno em job agendado: paga cluster e overhead por nada (custo, notebook 15).
# - pandas "porque o time conhece" para dado que cresce 10× ao ano: a reescrita chega.

# %% [markdown]
# ## 14. UDF Python × função nativa × pandas UDF (Arrow) — a ponte 🧪 / ☁️
#
# **O que é** — UDF (*user-defined function*) é código Python executado pelo Spark linha a linha. A UDF
# comum serializa cada linha da JVM para o Python e de volta. A **pandas UDF** (vetorizada) troca **lotes**
# em formato Arrow e processa uma `pd.Series` por vez.
#
# **Por que importa** — UDF Python é a causa nº 1 de "job lento sem motivo aparente": o otimizador não
# enxerga dentro dela e cada linha atravessa a fronteira JVM↔Python.
#
# **Como funciona** — mesma transformação (login em minúsculas) na bronze inteira, com função nativa e com
# UDF Python; o `noop` força o cálculo completo sem gravar nada.

# %%
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

bronze = spark.read.format("delta").load(s.path("bronze", "gh_events")).select(F.col("actor.login").alias("login"))
n_bronze = bronze.count()

lower_udf = udf(lambda x: x.lower() if x is not None else None, StringType())


def cronometrar(df) -> float:
    t0 = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    return time.perf_counter() - t0


cronometrar(bronze.select(F.lower("login")))  # aquece (cache de arquivos, JIT)
t_nat = cronometrar(bronze.select(F.lower("login").alias("l")))
t_udf = cronometrar(bronze.select(lower_udf("login").alias("l")))
print(f"{n_bronze:,} linhas | nativa {t_nat:.2f}s | UDF Python {t_udf:.2f}s ({t_udf / t_nat:.1f}× mais lenta)")
bronze.select(lower_udf("login").alias("l")).explain()

# %% [markdown]
# O plano mostra o nó `BatchEvalPython`: o ponto em que as linhas saem da JVM para o processo Python.
# Com pandas UDF ele vira `ArrowEvalPython` (lotes colunares). A pandas UDF **não roda aqui** porque o
# `pyarrow` não está instalado (saída da §13) — ☁️ no Databricks ele já vem no runtime:
#
# ```python
# import pandas as pd
# from pyspark.sql.functions import pandas_udf
#
# @pandas_udf("string")
# def lower_vec(s: pd.Series) -> pd.Series:      # recebe um lote (~10 mil linhas) por chamada
#     return s.str.lower()
#
# df.select(lower_vec("login"))
#
# # Spark 3.5+/4: UDF Python comum também pode trocar dados por Arrow
# @udf("string", useArrow=True)
# def lower_arrow(x): ...
# ```
#
# > 🎤 **Resposta de 30 s:** "Primeiro procuro função nativa do Spark — o otimizador enxerga e roda na JVM
# > (ou no Photon). Medi aqui: a UDF Python foi algumas vezes mais lenta que a nativa, e o plano mostra o
# > `BatchEvalPython`. Se preciso de Python (uma biblioteca, um modelo), uso pandas UDF: Arrow troca lotes
# > colunares e a função trabalha vetorizada."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Photon** (motor vetorizado em C++ do Databricks) não executa UDF Python: o trecho volta para o
#   caminho lento.
# - **Tipos de pandas UDF:** Series→Series, `Iterator[Series]→Iterator[Series]` (carrega modelo uma vez por
#   partição), `applyInPandas` para grupos (cuidado: um grupo inteiro precisa caber na memória de um worker).
# - **`spark.sql.execution.arrow.maxRecordsPerBatch`** controla o tamanho do lote Arrow.
# - **UDF em SQL** (`CREATE FUNCTION ... LANGUAGE PYTHON`) no Unity Catalog fica governada e reutilizável.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - pandas UDF ainda tem custo de serialização; nativa ganha sempre que existir.
# - Lote Arrow grande demais → OOM no worker Python, que é um processo separado do executor.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```python
# # 1) A lógica vai como wheel, declarada no Asset Bundle (databricks.yml) — notebook 13
# # resources:
# #   jobs:
# #     bronze:
# #       tasks:
# #         - task_key: ingest
# #           python_wheel_task: {package_name: oss_lakehouse, entry_point: cli}
# #           libraries: [{whl: ./dist/*.whl}]
#
# # 2) Segredos nunca no código: Key Vault via secret scope
# token = dbutils.secrets.get(scope="kv-osslh", key="github-token")
#
# # 3) Logs do driver vão para o Log Analytics (diagnostic settings) — o JSON da §10 vira colunas
# # 4) pandas e pyarrow já vêm no Databricks Runtime: pandas UDF funciona sem instalar nada
# # 5) Arquivos no ADLS: o Python puro lê via /Volumes/<catálogo>/<schema>/<volume>/... (Unity Catalog);
# #    o Spark lê direto abfss://lake@<conta>.dfs.core.windows.net/...
# ```
#
# - **Serverless jobs** sobem em segundos: o overhead da §13 diminui, mas não some.
# - **Free-threaded** não está nos runtimes do Databricks: a regra "CPU → processos/Spark" continua.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Qual a diferença entre iterator e generator? Quando um generator economiza memória?**
# <details><summary>Resposta</summary>
# Iterator é o protocolo (`__iter__`/`__next__`); generator é uma forma de implementá-lo com `yield`. Economiza
# memória quando o consumidor processa e descarta item a item — medido na §1: pico da lista inteira em centenas
# de MB contra menos de 1 MB do generator. Não economiza se você fizer `list(gen)`.
# </details>
#
# **2. Por que `itertools.groupby` "erra" a contagem?**
# <details><summary>Resposta</summary>
# Porque agrupa elementos consecutivos com a mesma chave. Sem ordenar pela chave, o mesmo valor gera vários
# grupos. Para contar, `Counter`; para agrupar em streaming, a entrada precisa vir ordenada.
# </details>
#
# **3. Escreva um decorator de retry. O que um sênior acrescenta?**
# <details><summary>Resposta</summary>
# Backoff exponencial com jitter, lista explícita de exceções re-tentáveis, teto de espera e de tentativas,
# `functools.wraps`, `ParamSpec`, `sleep` injetável para teste — e a pergunta "a operação é idempotente?".
# </details>
#
# **4. Como garantir que um consumidor nunca leia um arquivo pela metade?**
# <details><summary>Resposta</summary>
# Escrever em temporário e renomear atomicamente (`os.replace`, mesmo filesystem) dentro de um context manager
# que apaga o temporário em erro. Em object storage não há rename atômico: o Delta resolve com o transaction log.
# </details>
#
# **5. `Protocol` ou classe abstrata (ABC)?**
# <details><summary>Resposta</summary>
# `Protocol` é tipagem estrutural: qualquer classe com os métodos certos serve, sem herdar — ótimo para fakes em
# teste e para código de terceiros. ABC exige herança e pode carregar implementação padrão. Em fronteiras
# (sinks, clientes) prefiro Protocol.
# </details>
#
# **6. Dataclass ou pydantic para representar um evento?**
# <details><summary>Resposta</summary>
# Na fronteira (JSON externo), pydantic: valida, converte e devolve todos os erros. No miolo, dataclass
# (`frozen`, `slots`): leve e sem custo de validação. Em volume de big data, nenhum dos dois linha a linha:
# schema do Spark + regras de qualidade.
# </details>
#
# **7. Threads ou processos para baixar 500 arquivos? E para parsear 500 arquivos?**
# <details><summary>Resposta</summary>
# Baixar é espera de rede: threads (ou asyncio), com limite de concorrência. Parsear JSON em Python puro é CPU:
# processos, devolvendo resultado agregado. Medido na §8: parse com threads não acelera (piorou); com processos acelera.
# Em volume de verdade, Spark.
# </details>
#
# **8. O que é o GIL e o que muda com o Python free-threaded?**
# <details><summary>Resposta</summary>
# Lock que permite uma thread executando bytecode por vez. Extensões em C que o soltam (zlib, NumPy) paralelizam
# com threads. O build free-threaded (3.13 experimental, 3.14 suportado e opcional) remove o GIL, mas depende do
# suporte de cada extensão e expõe race conditions; eu mediria antes de adotar.
# </details>
#
# **9. Como você trata uma linha corrompida num arquivo de 10 milhões de linhas?**
# <details><summary>Resposta</summary>
# Isolo em quarentena com arquivo, linha e motivo, e o lote segue — com limiar de rejeição que faz o job falhar
# se a fonte quebrou inteira. Erro de sistema (credencial, rede) é outra classe: falha alto com retry.
# </details>
#
# **10. Por que sua UDF deixou o job 10× mais lento?**
# <details><summary>Resposta</summary>
# UDF Python serializa cada linha JVM↔Python e é opaca ao otimizador (sem pushdown, sem Photon). Troco por função
# nativa; se não houver, pandas UDF (Arrow, lotes colunares). O `explain()` mostra `BatchEvalPython`.
# </details>
#
# **11. pandas, Polars ou Spark para 2 GB de CSV diário?**
# <details><summary>Resposta</summary>
# Cabe numa máquina: Polars (ou pandas) resolve com menos custo e latência. Se o destino é o lakehouse
# governado e o volume cresce, Spark no Databricks — talvez em serverless para não pagar cluster ocioso.
# </details>
#
# **12. Como você empacota e entrega código Python para o Databricks?**
# <details><summary>Resposta</summary>
# Pacote com `pyproject.toml` e layout src, lock de dependências (uv), testes no CI, wheel versionada instalada no
# job pelo Asset Bundle. Notebook só orquestra; nada de lógica copiada entre notebooks.
# </details>

# %% [markdown]
# ## Resumo
#
# - **Memória:** generator mantém o pico do tamanho de um item — a lista inteira cresce com o arquivo (§1).
# - **Infra em Python:** decorator para o transversal (retry com jitter, tempo), context manager para limpeza
#   garantida (escrita atômica, `ExitStack`).
# - **Contratos:** typing para o CI, pydantic na fronteira, dataclass `frozen`/`slots` no miolo.
# - **Concorrência pelo gargalo:** I/O → threads/asyncio; CPU em Python → processos; volume → Spark (§8).
# - **No Spark:** função nativa > pandas UDF > UDF Python; lógica em wheel testada com pytest.

# %%
spark.stop()
