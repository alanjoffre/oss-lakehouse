# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 13 · Git, CI/CD e Declarative Automation Bundles
#
# > Este notebook prova o caminho do código até a produção: Git com histórico legível e reversível, versionamento
# > de pacote e de contrato, lint e testes automáticos, um wheel construído e um job multi-tarefa do Databricks
# > definido como código e promovido de dev para prod pelo GitHub Actions.
#
# | Requisito da vaga | Onde aparece aqui |
# |---|---|
# | Git e práticas de versionamento | §1 a §4 (trunk-based, Conventional Commits, SemVer, revert × reset, proteção de branch) |
# | Databricks (projetos com Databricks — diferencial) | §7 e §8 (`databricks.yml`, `resources/jobs.yml`) |
# | Arquitetura e desenvolvimento de pipelines | §6 (pirâmide de testes), §8 (job bronze → silver → gold → quality) |
# | Ambientes ágeis | §1, §9 (PR pequeno, CI em minutos, deploy contínuo em dev, release por tag) |
# | Python avançado | §6 e §7 (pytest, empacotamento com `uv build`) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

# %% [markdown]
# ## Setup
#
# Este notebook **não abre sessão Spark**: quem sobe Spark aqui é o `pytest` (§6), num processo próprio. Duas JVMs
# na mesma vaga de memória (`scripts/spark_slot.sh`) seria desperdício. Tudo que é destrutivo acontece em
# `data/demo/13/`. Os repositórios Git de demonstração usam datas fixas — os hashes saem iguais a cada build.

# %%
import os
import re
import shutil
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import yaml

from oss_lakehouse.config import PROJECT_ROOT, get_settings

ROOT = PROJECT_ROOT
DEMO = Path(get_settings().data_root) / "demo" / "13"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True, exist_ok=True)


ANSI = re.compile(r"\x1b\[[0-9;]*m")


def run(cmd: list[str], cwd: Path = ROOT, env: dict | None = None, tail: int = 25) -> None:
    """Roda um comando e imprime o código de saída e as últimas linhas (stdout+stderr), sem cores."""
    env = (env or dict(os.environ)) | {"NO_COLOR": "1"}
    env.pop("FORCE_COLOR", None)
    p = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    nome = Path(cmd[0]).name + " " + " ".join(cmd[1:])
    print(f"$ {nome.replace(str(ROOT) + '/', '')}   → exit {p.returncode}")
    for linha in ANSI.sub("", p.stdout + p.stderr).strip().splitlines()[-tail:]:
        print("  " + linha.replace(str(ROOT) + "/", ""))


# Ambiente limpo para o git de demonstração: sem config global do usuário, sem GIT_* herdado.
GIT_ENV = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
GIT_ENV |= {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
_relogio = iter(range(1_000))


def git(repo: Path, *args: str, show: bool = False) -> str | None:
    """git com autor e data fixos (hash reprodutível)."""
    t = f"2026-10-01T12:{next(_relogio):02d}:00+00:00"
    env = GIT_ENV | {"GIT_AUTHOR_DATE": t, "GIT_COMMITTER_DATE": t}
    base = ["git", "-c", "user.name=Dev", "-c", "user.email=dev@example.com", "-c", "init.defaultBranch=main",
            "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"]
    p = subprocess.run(base + list(args), cwd=repo, env=env, capture_output=True, text=True, check=True)
    if show:
        print(p.stdout.rstrip())
        return None
    return p.stdout


def commit(repo: Path, arquivo: str, conteudo: str, msg: str) -> None:
    (repo / arquivo).parent.mkdir(parents=True, exist_ok=True)
    (repo / arquivo).write_text(conteudo)
    git(repo, "add", arquivo)
    git(repo, "commit", "-q", "-m", msg)


print(f"git: {subprocess.run(['git', '--version'], capture_output=True, text=True).stdout.strip()}")

# %% [markdown]
# ## 1. Estratégia de branches: trunk-based × GitFlow 🧪
#
# **O que é**
#
# - **GitFlow:** branches de longa duração (`develop`, `release/*`, `hotfix/*`) além da `main`; integração
#   acontece em momentos de release.
# - **Trunk-based development** (*desenvolvimento no tronco*): todo mundo integra numa única branch (`main`) com
#   frequência — branches de feature vivem horas ou poucos dias, entram por PR pequeno e o CI garante que a `main`
#   está sempre implantável.
#
# **Por que importa** — Em dados, branch longa é especialmente cara: o schema da tabela e o dado de produção
# continuam mudando enquanto a branch envelhece; o merge final vira migração de dados às cegas. Trunk-based +
# CI + deploy automático em `dev` mantém o código perto do dado real. Funcionalidade inacabada fica atrás de
# *feature flag* (configuração que liga/desliga), não numa branch.
#
# **Como funciona** — Um repositório de demonstração: `main` com a ingestão, uma branch curta de feature e uma
# correção que entrou na `main` enquanto isso.

# %%
base = DEMO / "repo_base"
base.mkdir()
git(base, "init", "-q")
commit(base, "src/bronze.py", "# ingestão\n", "chore: estrutura inicial do pacote")
commit(base, "src/bronze.py", "# ingestão incremental\n", "feat(bronze): ingestão incremental com checkpoint")
git(base, "tag", "v0.1.0")
git(base, "switch", "-q", "-c", "feat/silver-dedup")
commit(base, "src/silver.py", "# dedup por id\n", "feat(silver): deduplicação por id do evento")
commit(base, "tests/test_silver.py", "# teste\n", "test(silver): duplicata entre arquivos")
git(base, "switch", "-q", "main")
commit(base, "src/bronze.py", "# ingestão incremental\n# org nulo ok\n", "fix(bronze): aceitar org nulo no envelope")
git(base, "log", "--graph", "--oneline", "--all", "--decorate", show=True)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Uso trunk-based: branches curtas, PR pequeno com revisão e CI, merge na `main`
# > várias vezes por semana, `main` sempre implantável e deploy automático em dev. GitFlow faz sentido para
# > software com versões paralelas em campo; em pipeline de dados, branch longa diverge do schema e do dado de
# > produção e o merge vira migração arriscada."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Mudança de schema incompatível em trunk-based:** padrão *expand/contract* — 1) adiciona a coluna nova
#   (compatível), 2) migra consumidores, 3) remove a antiga num release posterior. Cada passo é um PR pequeno.
# - **Feature flag em pipeline:** parâmetro do job/variável do bundle que liga a lógica nova só em dev;
#   em prod, liga depois de validada.
# - **Métricas DORA:** frequência de deploy, *lead time*, taxa de falha de mudança e tempo de recuperação —
#   trunk-based melhora as quatro.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Trunk-based exige CI rápido e confiável e testes de verdade; sem isso, a `main` quebra com frequência.
# - Produtos com várias versões suportadas ao mesmo tempo (biblioteca, app instalado) precisam de branches de
#   release — GitFlow ou *release branches*.

# %% [markdown]
# ## 2. Rebase × merge 🧪
#
# **O que é** — Duas formas de integrar a branch de feature com a `main` que andou:
#
# - **merge:** cria um *merge commit* com dois pais; preserva exatamente o que aconteceu.
# - **rebase:** reaplica os commits da feature **em cima** da `main` atual; histórico linear, mas os commits ganham
#   hashes novos (o histórico é reescrito).
#
# **Por que importa** — Histórico linear facilita `git bisect` (busca binária do commit que quebrou) e leitura.
# Mas reescrever commits que outra pessoa já baixou cria divergência e retrabalho.
#
# **Como funciona** — As duas operações sobre cópias do mesmo repositório:

# %%
for estrategia in ("merge", "rebase"):
    repo = DEMO / f"repo_{estrategia}"
    shutil.copytree(base, repo)
    if estrategia == "merge":
        git(repo, "merge", "-q", "--no-ff", "feat/silver-dedup", "-m", "Merge branch 'feat/silver-dedup'")
    else:
        git(repo, "switch", "-q", "feat/silver-dedup")
        git(repo, "rebase", "-q", "main")
        git(repo, "switch", "-q", "main")
        git(repo, "merge", "-q", "--ff-only", "feat/silver-dedup")
    print(f"--- {estrategia}")
    git(repo, "log", "--graph", "--oneline", "main", show=True)

# %% [markdown]
# No merge, a história mostra a bifurcação; no rebase, os dois commits da feature aparecem depois do `fix` com
# **hashes diferentes** dos originais — são commits novos.
#
# > 🎤 **Resposta de 30 s:** "Na minha branch local, rebase para manter o histórico limpo; em branch
# > compartilhada, nunca reescrevo. No PR, prefiro *squash merge* ou *rebase merge* para a `main` ficar linear,
# > com um commit por mudança lógica — facilita revert e bisect."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Regra de ouro do rebase:** não rebasear commits já publicados que outros usam. Se precisar, `push
#   --force-with-lease` (falha se o remoto tiver algo que você não viu), nunca `--force` puro.
# - **Squash merge:** o PR inteiro vira um commit na `main`; perde-se o detalhe dos commits intermediários, ganha-se
#   uma linha por mudança. Combina com Conventional Commits no título do PR.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Rebase com muitos conflitos repetidos: `git rerere` ajuda; às vezes merge é mais honesto.
# - Merge commits em excesso ("Merge main into feature" a cada dia) poluem o histórico sem informação.

# %% [markdown]
# ## 3. Conventional Commits e versionamento semântico — do pacote e do contrato 🧪
#
# **O que é**
#
# - **Conventional Commits:** convenção de mensagem `tipo(escopo): descrição` — `feat`, `fix`, `docs`, `test`,
#   `refactor`, `chore`, `ci`; `!` ou `BREAKING CHANGE:` marcam quebra de compatibilidade.
# - **SemVer** (*versionamento semântico*) `MAJOR.MINOR.PATCH`: MAJOR quebra compatibilidade, MINOR adiciona sem
#   quebrar, PATCH corrige.
#
# **Por que importa** — A mensagem vira dado: a próxima versão e o *changelog* saem dos commits, sem discussão.
# E em dados há **dois** artefatos versionados: o **pacote** (código) e o **contrato da tabela** (schema, grão,
# semântica) — quem consome a tabela precisa saber se a mudança o quebra.
#
# **Como funciona** — Próxima versão do pacote a partir dos commits desde a última tag:

# %%
BUMP = {"major": 3, "minor": 2, "patch": 1, None: 0}


def nivel(msg: str) -> str | None:
    cabecalho = msg.splitlines()[0]
    if re.match(r"^\w+(\(.+\))?!:", cabecalho) or "BREAKING CHANGE:" in msg:
        return "major"
    if cabecalho.startswith("feat"):
        return "minor"
    if cabecalho.startswith(("fix", "perf")):
        return "patch"
    return None  # docs, test, chore, ci, refactor: não geram release


def proxima_versao(atual: str, mensagens: list[str]) -> str:
    major, minor, patch = map(int, atual.lstrip("v").split("."))
    n = max((nivel(m) for m in mensagens), key=lambda x: BUMP[x], default=None)
    if n == "major":
        return f"v{major + 1}.0.0"
    if n == "minor":
        return f"v{major}.{minor + 1}.0"
    if n == "patch":
        return f"v{major}.{minor}.{patch + 1}"
    return atual


repo = DEMO / "repo_rebase"
msgs = git(repo, "log", "--format=%B%x00", "v0.1.0..main").split("\x00")
msgs = [m.strip() for m in msgs if m.strip()]
for m in msgs:
    print(f"{nivel(m) or '-':<6} {m.splitlines()[0]}")
print(f"\nv0.1.0 → {proxima_versao('v0.1.0', msgs)}")
print("com 'feat(silver)!: id passa a ser BIGINT' →",
      proxima_versao("v0.1.0", [*msgs, "feat(silver)!: id passa a ser BIGINT"]))

# %% [markdown]
# Para o **contrato da tabela**, a mesma lógica aplicada ao schema: remover coluna ou mudar tipo quebra o
# consumidor (MAJOR); coluna nova anulável não quebra (MINOR); mudar só a descrição é PATCH. Comparando o envelope
# da bronze (`GH_EVENT_SCHEMA`) com duas evoluções hipotéticas — `StructType` é Python puro, não precisa de Spark:

# %%
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

from oss_lakehouse.bronze import GH_EVENT_SCHEMA  # noqa: E402


def mudanca_de_contrato(antigo: StructType, novo: StructType) -> tuple[str, list[str]]:
    a = {f.name: f for f in antigo.fields}
    n = {f.name: f for f in novo.fields}
    motivos, nivel_ = [], "patch"
    for nome in a.keys() - n.keys():
        motivos.append(f"coluna removida: {nome}")
        nivel_ = "major"
    for nome in a.keys() & n.keys():
        if a[nome].dataType != n[nome].dataType:
            motivos.append(f"tipo mudou: {nome} {a[nome].dataType.simpleString()[:30]} → "
                           f"{n[nome].dataType.simpleString()[:30]}")
            nivel_ = "major"
        elif a[nome].nullable and not n[nome].nullable:
            motivos.append(f"passou a NOT NULL: {nome}")
            nivel_ = "major"
    for nome in n.keys() - a.keys():
        motivos.append(f"coluna nova: {nome} (anulável={n[nome].nullable})")
        if nivel_ != "major":
            nivel_ = "minor" if n[nome].nullable else "major"
    return nivel_, motivos


com_coluna = StructType([*GH_EVENT_SCHEMA.fields, StructField("payload_version", StringType(), True)])
sem_public = StructType([f for f in GH_EVENT_SCHEMA.fields if f.name != "public"])
for nome, novo in [("adiciona payload_version", com_coluna), ("remove public", sem_public)]:
    n, motivos = mudanca_de_contrato(GH_EVENT_SCHEMA, novo)
    print(f"{nome:<26} → {n.upper():<5} {motivos}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Uso Conventional Commits para que a versão e o changelog saiam do histórico:
# > `feat` sobe MINOR, `fix` sobe PATCH, `!` sobe MAJOR. E versiono também o contrato da tabela: coluna nova
# > anulável é MINOR; remover coluna ou mudar tipo é MAJOR e exige aviso aos consumidores e período de convivência."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Automação:** `commitlint` valida a mensagem no PR; *release-please* ou *semantic-release* abrem o PR de
#   release com versão e changelog calculados.
# - **Breaking change de tabela na prática:** nova coluna com o tipo novo + view de compatibilidade com o nome
#   antigo, ou tabela `_v2` em paralelo; data de remoção anunciada. O contrato (notebook 08) é verificado no CI.
# - **Versão do pacote dentro do dado:** gravar a versão do código que produziu cada lote (ex.: em
#   `_pipeline_version`) torna a linhagem auditável.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Convenção sem verificação automática degrada em semanas; ou se automatiza, ou não se adota.
# - Mudança semântica sem mudança de schema (ex.: `amount` passa de centavos para reais) não aparece no diff de
#   schema — é MAJOR e só uma pessoa percebe. Por isso o contrato tem descrição e dono.

# %% [markdown]
# ## 4. Desfazer: revert × reset (e o reflog) 🧪
#
# **O que é**
#
# - **`git revert <commit>`:** cria um commit **novo** que desfaz o anterior. O histórico é preservado — seguro em
#   branch compartilhada.
# - **`git reset --hard <commit>`:** move a branch para trás, **descartando** commits. Reescreve histórico —
#   só em trabalho local, nunca publicado.
# - **`git reflog`:** diário local de onde o `HEAD` esteve — recupera o que um reset "apagou".
#
# **Por que importa** — Quando um deploy quebra produção, o caminho seguro é `revert` + deploy (o CI roda de novo,
# todo mundo vê o que foi desfeito). `reset` + `push --force` na `main` apaga o trabalho dos outros.
#
# **Como funciona**

# %%
repo = DEMO / "repo_rebase"
commit(repo, "src/gold.py", "# agregado com join errado\n", "feat(gold): agregado diário por repositório")
ruim = git(repo, "rev-parse", "--short", "HEAD").strip()

reverter = DEMO / "repo_revert"
shutil.copytree(repo, reverter)
git(reverter, "revert", "--no-edit", ruim)
print("--- revert (histórico preservado, commit novo desfaz o anterior)")
git(reverter, "log", "--oneline", "-3", show=True)

resetar = DEMO / "repo_reset"
shutil.copytree(repo, resetar)
git(resetar, "reset", "-q", "--hard", "HEAD~1")
print("\n--- reset --hard (o commit sumiu da branch)")
git(resetar, "log", "--oneline", "-2", show=True)
print("\n--- mas o reflog ainda sabe onde ele está:")
git(resetar, "reflog", "-2", show=True)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Em branch compartilhada, desfaço com `revert`: commit novo, histórico intacto, CI e
# > deploy rodam de novo. `reset` só no que é meu e não foi publicado. E se alguém fez reset por engano, o `reflog`
# > recupera o commit. Em dados, lembrar que reverter o código não reverte a tabela — para isso há time travel e
# > `RESTORE` do Delta (notebook 10)."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`reset --soft` / `--mixed`:** voltam a branch mas mantêm as mudanças no índice/diretório — útil para refazer
#   commits locais.
# - **Reverter um merge:** `git revert -m 1 <merge>` (escolhe o pai que fica). Re-mergear a mesma branch depois
#   exige reverter o revert.
# - **Rollback de dados:** `RESTORE TABLE ... TO VERSION AS OF n` volta a tabela; o job seguinte precisa do código
#   corrigido, senão reescreve o erro.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - `revert` de um commit antigo pode conflitar com o que veio depois; às vezes é mais simples um *fix forward*
#   (corrigir para a frente) com um PR pequeno.

# %% [markdown]
# ## 5. PR com revisão, proteção de branch e pre-commit 🧪/☁️
#
# **O que é**
#
# - **Pull request (PR) com revisão:** a mudança entra na `main` só depois de outra pessoa ler e do CI passar.
# - **Proteção de branch** (no GitHub, *rulesets*): regras na `main` — PR obrigatório, N aprovações, checks do CI
#   obrigatórios, histórico linear, proibido *force push* e exclusão; `CODEOWNERS` exige o dono da área.
# - **pre-commit:** ganchos que rodam no `git commit` local e barram erro barato em segundos (lint, YAML
#   inválido, chave privada, arquivo gigante).
#
# **Por que importa** — Revisão pega erro de lógica e espalha conhecimento; proteção de branch garante que a regra
# vale mesmo com pressa; pre-commit tira do CI (minutos) o que dá para pegar em segundos.
#
# **Como funciona** — ☁️ ruleset da `main` via API do GitHub (não executado aqui — não há repositório remoto):
#
# ```bash
# gh api repos/<dono>/oss-lakehouse/rulesets -X POST --input - <<'JSON'
# {
#   "name": "main protegida", "target": "branch", "enforcement": "active",
#   "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
#   "rules": [
#     {"type": "deletion"}, {"type": "non_fast_forward"}, {"type": "required_linear_history"},
#     {"type": "pull_request", "parameters": {"required_approving_review_count": 1,
#       "require_code_owner_review": true, "dismiss_stale_reviews_on_push": true,
#       "required_review_thread_resolution": true, "require_last_push_approval": false}},
#     {"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": true,
#       "required_status_checks": [{"context": "lint"}, {"context": "test"}, {"context": "terraform"}]}}
#   ]
# }
# JSON
# ```
#
# 🧪 Os ganchos configurados em `.pre-commit-config.yaml`:

# %%
cfg = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text())
for r in cfg["repos"]:
    origem = r["repo"].removeprefix("https://github.com/")
    print(f"{origem:<34} {r.get('rev', ''):<9} {[h['id'] for h in r['hooks']]}")
print("\nnbstripout configurado?", any("nbstripout" in r["repo"] for r in cfg["repos"]))

# %% [markdown]
# **Por que não há `nbstripout`** (gancho que apaga as saídas dos notebooks antes do commit): aqui as saídas
# **são o produto** — quem abre o repositório no GitHub vê o resultado sem rodar nada. A garantia de que a saída é
# real vem do processo: o `.ipynb` é gerado e executado por `scripts/build_notebooks.py` a partir de
# `notebooks/_src/*.py`, que é o que se revisa no PR. Num repositório de produção, o padrão é o contrário:
# `nbstripout` ligado, porque saída versionada vaza dado e polui o diff.
#
# O lint que o gancho e o CI rodam, executado agora:

# %%
run([sys.executable, "-m", "ruff", "check", "src", "tests", "scripts"])

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "A `main` é protegida: só entra por PR com uma aprovação, dono da área revisando,
# > checks do CI obrigatórios e sem force push. Localmente, pre-commit roda ruff e checagens de YAML e segredo no
# > commit — o CI repete as mesmas checagens porque gancho local se desliga com `--no-verify`."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **O que revisar num PR de dados:** idempotência (rodar duas vezes duplica?), chave de MERGE, tratamento de
#   nulo e de dado atrasado, mudança de contrato, custo (full scan? explode?), teste cobrindo o caso.
# - **PR pequeno:** < 400 linhas alteradas se revisa de verdade; PR de 3.000 linhas recebe "LGTM".
# - **IA na revisão:** assistentes de código (Copilot, Claude, Databricks Assistant) ajudam a revisar e a escrever
#   testes, mas a aprovação continua humana e o CI continua obrigatório — mesma disciplina do ADR 0006.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Regras demais num time de duas pessoas viram atrito; comece por "PR + CI obrigatório + sem force push".
# - pre-commit lento (testes com Spark no gancho) faz as pessoas usarem `--no-verify`; teste pesado fica no CI.

# %% [markdown]
# ## 6. Pirâmide de testes para dados 🧪
#
# **O que é** — Camadas de teste, da mais barata e numerosa para a mais cara e rara:
#
# ```mermaid
# flowchart TB
#     E2E["Ponta a ponta<br/>job inteiro no alvo dev do bundle; make demo"] --> DQ
#     DQ["Testes de dados / contrato<br/>expectations, schema, frescor, volume — rodam em produção, a cada lote"] --> INT
#     INT["Integração<br/>Spark local + Delta: ingestão idempotente, MERGE"] --> UNIT
#     UNIT["Unitários<br/>funções puras e transformações pequenas (chispa)"]
# ```
#
# | Camada | Exemplo neste repo | Custo |
# |---|---|---|
# | Unitário | `hour_keys`, `retry` (`tests/test_foundation.py`); transformações com `chispa.assert_df_equality` (`tests/test_silver.py`) | milissegundos |
# | Integração | `test_bronze_e_idempotente`: grava Delta de verdade, roda 2×, confere que não duplica | segundos (JVM) |
# | Dados/contrato | expectations e contratos (notebook 08, `oss_lakehouse.quality`) | por lote, em produção |
# | Ponta a ponta | `make demo`; job `medallion` no alvo `dev` | minutos |
#
# **Por que importa** — Teste de código prova que a **lógica** está certa com dado conhecido; teste de dados prova
# que o **dado de hoje** está dentro do esperado. Pipeline precisa dos dois: código perfeito com fonte quebrada
# ainda publica lixo.
#
# **Como funciona** — Os testes da fundação, com o tempo de cada um (`--durations`): a diferença de custo entre
# unitário e integração é a pirâmide em números.

# %%
env_teste = os.environ | {"SPARK_LOCAL_IP": "127.0.0.1"}
env_teste.pop("VIRTUAL_ENV", None)
run([sys.executable, "-m", "pytest", "tests/test_foundation.py", "-q", "--color=no", "--durations=0",
     "-p", "no:cacheprovider"],
    env=env_teste, tail=20)

# %% [markdown]
# Os três testes unitários nem aparecem na lista: o pytest esconde durações abaixo de 5 ms. O de integração gasta
# segundos só no *setup* (subir a JVM e resolver o JAR do Delta) e outros tantos gravando Delta de verdade — é a
# pirâmide em números.
#
# Um teste unitário de transformação com `chispa` (compara DataFrames e mostra a diferença linha a linha):
#
# ```python
# from chispa import assert_df_equality
# from pyspark.sql import functions as F
# from oss_lakehouse.silver import deduplicate_latest
#
# def test_dedup_mantem_o_mais_recente(spark):
#     schema = "id string, created_at string"
#     entrada = spark.createDataFrame([("1", "2026-10-01T12:00:00Z"), ("1", "2026-10-01T12:05:00Z")], schema)
#     esperado = spark.createDataFrame([("1", "2026-10-01T12:05:00Z")], schema)
#     obtido = deduplicate_latest(entrada, ["id"], [F.col("created_at").desc()])
#     assert_df_equality(obtido, esperado, ignore_row_order=True)
# ```
#
# > 🎤 **Resposta de 30 s:** "Muitos testes unitários de funções puras e transformações pequenas com chispa;
# > alguns de integração com Spark local e Delta de verdade, provando idempotência; testes de dados e contrato
# > rodando em produção a cada lote; e um ponta a ponta no ambiente dev. O teste de integração custa milhares de
# > vezes o unitário — por isso a base da pirâmide é larga."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Sessão Spark por suíte** (`scope="session"` no `conftest.py`): subir a JVM custa segundos; uma por teste
#   tornaria a suíte inviável.
# - **Dado de teste:** amostra real pequena e versionada (`tests/fixtures/gharchive`, 2.000 eventos) + casos
#   construídos para as bordas (nulo, duplicata, atraso). Nunca dado de produção com PII.
# - **Teste de propriedade:** "rodar duas vezes dá o mesmo resultado" (idempotência) pega mais bug de pipeline do
#   que comparar com um resultado fixo.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Mock de Spark/DataFrame testa o mock, não o Spark; prefira Spark local de verdade.
# - Teste ponta a ponta frágil (depende de rede, horário) desacredita o CI; isole e marque.

# %% [markdown]
# ## 7. Empacotamento: o wheel que o job executa 🧪
#
# **O que é** — Um **wheel** (`.whl`) é o pacote Python construído, pronto para instalar. O job do Databricks
# instala o wheel e chama um *entry point* (função registrada nos metadados do pacote).
#
# **Por que importa** — Notebook com `%run` não tem versão, não tem teste e não tem dependência declarada. O wheel
# tem os três: o job de prod roda **exatamente** o artefato que passou no CI.
#
# **Como funciona** — `uv build` lê o `pyproject.toml` e gera o wheel (o mesmo comando está no `databricks.yml`):

# %%
dist = DEMO / "dist"
uv = shutil.which("uv") or str(Path.home() / ".local" / "bin" / "uv")
run([uv, "build", "--wheel", "--out-dir", str(dist)], tail=3)
whl = next(dist.glob("*.whl"))
print(f"\n{whl.name}  {whl.stat().st_size / 1024:.0f} KiB")
with zipfile.ZipFile(whl) as z:
    nomes = z.namelist()
    modulos = sorted(n for n in nomes if n.endswith(".py"))
    print(f"{len(modulos)} módulos .py, ex.: {[m.split('/', 1)[1] for m in modulos[:6]]}")
    meta = z.read(next(n for n in nomes if n.endswith("dist-info/METADATA"))).decode()
    tem_entry_points = any(n.endswith("dist-info/entry_points.txt") for n in nomes)
requires = [linha.split(": ", 1)[1] for linha in meta.splitlines() if linha.startswith("Requires-Dist")]
print("Requires-Dist:", requires)
print("entry_points.txt no wheel?", tem_entry_points)

# %% [markdown]
# Duas coisas que esta inspeção revela — e que só aparecem quando se olha o artefato, não o código:
#
# 1. **`pyspark` e `delta-spark` são dependências de instalação do wheel.** No cluster, o pip instalaria um
#    `pyspark` do PyPI por cima do Spark do runtime — o clássico "funciona local, quebra no Databricks". O certo é
#    tirá-los de `dependencies` e deixá-los num grupo só de desenvolvimento/local (ex.: `[dependency-groups] local`
#    ou um *extra*), como faz o template oficial do bundle.
# 2. **Sem `entry_points.txt`:** o `python_wheel_task` do job chama o entry point `oss-lakehouse`, que precisa
#    existir em `[project.scripts]` (`oss-lakehouse = "oss_lakehouse.cli:main"`). Sem isso, o job falha ao iniciar.
#
# Os dois ajustes vão no `pyproject.toml` (arquivo compartilhado do repositório).
#
# > 🎤 **Resposta de 30 s:** "O pipeline é um wheel construído com `uv build` no CI e publicado pelo bundle; o
# > job chama um entry point do pacote. Cuidado clássico: não declarar `pyspark` como dependência do wheel — no
# > Databricks o Spark vem do runtime, e reinstalar quebra o cluster."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Lockfile:** `uv.lock` fixa versões para o CI e o laptop; no wheel, só faixas (`>=`), para não brigar com o
#   runtime. Dependências do runtime se consultam nas *release notes* do DBR.
# - **Onde o wheel vai:** o bundle sobe para o workspace (`.bundle/<nome>/<alvo>/artifacts`) ou para um Volume;
#   para compartilhar entre projetos, um feed privado (Azure Artifacts) é mais limpo.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Para um notebook exploratório isolado, wheel é cerimônia demais; vale quando há job agendado e teste.

# %% [markdown]
# ## 8. Declarative Automation Bundles: o job como código 🧪/☁️
#
# **O que é** — **Declarative Automation Bundles** (nome desde mar/2026; antes *Databricks Asset Bundles*, "DABs" —
# mesmos comandos e arquivos) descrevem em YAML os recursos do Databricks (jobs, pipelines, permissões) junto com o
# código, com **alvos** (*targets*) por ambiente. O CLI valida, constrói o wheel, faz upload e cria/atualiza os
# recursos ([release notes](https://docs.databricks.com/aws/en/release-notes/dev-tools/bundles)).
#
# **Por que importa** — Job criado na UI não tem revisão, histórico nem promoção entre ambientes. Com bundle, o
# job de prod é idêntico ao de dev, a menos das variáveis — e qualquer mudança passa por PR.
#
# **Como funciona** — `databricks.yml` (alvos, variáveis, artefato) + `resources/jobs.yml` (o job):
#
# | Alvo | `mode` | O que o modo faz |
# |---|---|---|
# | `dev` | `development` | prefixo `[dev <usuário>]` nos recursos, schedules pausados, execuções concorrentes, sem lock de deploy |
# | `prod` | `production` | valida que não há caminho pessoal; `run_as` = **service principal**; permissões explícitas; cluster não pode ser sobrescrito na linha de comando |
#
# Fonte: [deployment modes](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes).
#
# ```mermaid
# flowchart LR
#     B["bronze<br/>retries 2 · 30 min"] --> S["silver<br/>retries 2 · 30 min"] --> G["gold<br/>retries 1 · 30 min"] --> Q["quality<br/>retries 0 · 15 min"]
# ```
#
# O DAG lido do próprio YAML (não do diagrama):

# %%
bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
recursos = {}
for padrao in bundle.get("include", []):
    for arq in sorted(ROOT.glob(padrao)):
        recursos |= yaml.safe_load(arq.read_text())["resources"]["jobs"]
job = recursos["medallion"]
print(f"bundle={bundle['bundle']['name']}  alvos={list(bundle['targets'])}  "
      f"artefato='{bundle['artifacts']['oss_lakehouse']['build']}'")
print(f"job={job['name']}  agenda='{job['schedule']['quartz_cron_expression']}' {job['schedule']['timezone_id']}  "
      f"max_concurrent_runs={job['max_concurrent_runs']}  timeout={job['timeout_seconds']}s\n")
print(f"{'task':<8} {'depende de':<11} {'comando':<9} {'retries':>7} {'timeout':>8}")
for t in job["tasks"]:
    dep = ",".join(d["task_key"] for d in t.get("depends_on", [])) or "-"
    print(f"{t['task_key']:<8} {dep:<11} {t['python_wheel_task']['parameters'][0]:<9} "
          f"{t.get('max_retries', 0):>7} {t['timeout_seconds']:>7}s")
print("\nprod roda como:", bundle["targets"]["prod"]["run_as"])
print("prod sobrescreve o cluster:", {k: bundle["targets"]["prod"]["variables"]["cluster"][k]
                                    for k in ("node_type_id", "runtime_engine", "autoscale")})

# %% [markdown]
# **Validação.** Sem workspace, o `databricks bundle validate` não passa da autenticação — é esperado (o CLI
# precisa do host para resolver `workspace.current_user` e os caminhos). O que dá para validar offline é o
# **esquema**: o próprio CLI exporta o JSON Schema oficial (`databricks bundle schema`), e validamos os dois
# arquivos contra ele. Para provar que a validação morde, injetamos dois erros de digitação.

# %%
databricks = shutil.which("databricks") or str(Path.home() / ".local" / "bin" / "databricks")
tem_cli = Path(databricks).exists()
if tem_cli:
    versao = subprocess.run([databricks, "--version"], capture_output=True, text=True).stdout.strip()
    print(versao)
    env_sem_host = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_")}
    p = subprocess.run([databricks, "bundle", "validate", "-t", "dev"], cwd=ROOT, env=env_sem_host,
                       capture_output=True, text=True)
    print(f"$ databricks bundle validate -t dev   → exit {p.returncode}")
    for linha in (p.stdout + p.stderr).strip().splitlines():
        if "aitools" not in linha and "skills are not installed" not in linha:  # dica de produto, não erro
            print("  " + linha[:160])
else:
    print("☁️ CLI do Databricks não instalado nesta máquina — pule para a validação só de YAML abaixo")

# %%
import json  # noqa: E402

import jsonschema  # noqa: E402

if tem_cli:
    bruto = subprocess.run([databricks, "bundle", "schema"], capture_output=True, text=True, check=True).stdout
    # O schema usa classes Unicode do Go (\p{L}, \p{N}) nos padrões de ${...}; o `re` do Python não as conhece.
    bruto = bruto.replace(r"[\\p{L}\\p{N}]", "[A-Za-z0-9]").replace(r"\\p{L}", "[A-Za-z]")
    schema = json.loads(bruto)
    validador = jsonschema.validators.validator_for(schema)(schema)

    def folhas(e: jsonschema.ValidationError):
        """Erros-folha de um oneOf. Cada campo aceita o valor OU uma string `${var...}` — a alternativa
        "string de interpolação" sempre falha junto e não interessa."""
        interpolacao = (e.validator == "type" and e.validator_value == "string") or e.validator == "pattern"
        if not e.context and not interpolacao:
            yield e
        for c in e.context:
            yield from folhas(c)

    def erros(doc: dict) -> list[str]:
        vistos = {f"{'.'.join(map(str, f.absolute_path))}: {f.message[:95]}"
                  for e in validador.iter_errors(doc) for f in folhas(e)}
        return sorted(vistos)

    print(f"schema oficial: {len(bruto) / 1e6:.1f} MB")
    for arq in ["databricks.yml", *sorted(str(p.relative_to(ROOT)) for p in ROOT.glob("resources/*.yml"))]:
        print(f"{arq:<20} erros de esquema: {len(erros(yaml.safe_load((ROOT / arq).read_text())))}")

    quebrado = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text())
    quebrado["resources"]["jobs"]["medallion"]["tasks"][0]["max_retrys"] = 2               # typo no nome
    quebrado["resources"]["jobs"]["medallion"]["schedule"]["pause_status"] = "PAUSADO"     # valor fora do enum
    print("\ncom 2 erros injetados:")
    for linha in erros(quebrado):
        print("  ", linha)
else:
    for arq in ["databricks.yml", "resources/jobs.yml"]:
        yaml.safe_load((ROOT / arq).read_text())
        print(f"{arq}: YAML sintaticamente válido (sem CLI, sem validação de esquema)")

# %% [markdown]
# **Variante serverless (Free Edition) ☁️** — a Free Edition só tem serverless, então `job_clusters` não existe lá.
# Cada task troca `job_cluster_key` + `libraries` por um `environment_key`, e o wheel vira dependência do ambiente
# (mesma estrutura do template oficial `default-python`):
#
# ```yaml
# resources:
#   jobs:
#     medallion:
#       tasks:
#         - task_key: bronze
#           environment_key: default
#           python_wheel_task: {package_name: oss_lakehouse, entry_point: oss-lakehouse, parameters: ["bronze"]}
#       environments:
#         - environment_key: default
#           spec:
#             environment_version: "6"      # Python 3.12, set/2026
#             dependencies: ["../dist/*.whl"]
# ```
#
# **Por que não há `resources/pipelines.yml`:** o job chama funções do pacote (código imperativo testado com
# pytest). Um pipeline de **Lakeflow Spark Declarative Pipelines** (SDP, ex-DLT) faria sentido se a silver/gold
# fossem declaradas como *streaming tables* e *materialized views* com expectations — é o tema do notebook 08.
# Ter as duas formas para a mesma tabela seria duas fontes da verdade.
#
# > 🎤 **Resposta de 30 s:** "O job é código: `databricks.yml` com alvos dev e prod, variáveis e o wheel; o
# > job multi-tarefa em `resources/jobs.yml` com dependências, job cluster compartilhado, retries só onde a task é
# > idempotente, timeout, alerta de duração e agenda. Dev tem prefixo por pessoa e agenda pausada; prod roda como
# > service principal com permissões explícitas. O CI valida e faz o deploy."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Retry só onde é seguro:** bronze (checkpoint) e silver (MERGE) são idempotentes → 2 retries; `quality` tem 0
#   porque reprovação é sinal, não falha transitória.
# - **`quality` depois da `gold`:** aqui a checagem roda sobre o que já foi publicado e alerta. A alternativa mais
#   rigorosa é *write-audit-publish*: gravar a gold numa área de staging, auditar, e só então publicar (troca
#   atômica). Custa uma cópia; vale para tabela crítica.
# - **Motor de deploy:** desde o CLI 1.3.0 (jun/2026) bundles novos usam o *direct deployment engine* em vez do
#   Terraform por baixo — mais rápido, mesmo YAML.
# - **`databricks bundle plan`** mostra o que o deploy vai criar/alterar/apagar antes de aplicar — como
#   `terraform plan`.
# - **Variável complexa** (`var.cluster`): o cluster inteiro é uma variável; prod sobrescreve tamanho e Photon sem
#   duplicar o job.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Bundle não substitui o Terraform da infraestrutura (workspace, storage, rede — notebook 14); um cuida do que
#   roda **dentro** do workspace, o outro de **onde** ele roda.
# - Para um job único e estável, o overhead de bundle + CI pode não se pagar — mas raramente fica único.

# %% [markdown]
# ## 9. GitHub Actions: CI e deploy por ambiente 🧪/☁️
#
# **O que é** — O workflow `.github/workflows/ci.yml` roda a cada PR e push: lint, testes com Spark (Java 17 + uv),
# `terraform fmt`/`validate`, validação do bundle e deploy — `dev` a cada push na `main`, `prod` a cada tag `v*`,
# com aprovação manual.
#
# **Por que importa** — CI é o que torna trunk-based seguro; CD por ambiente é o que torna o deploy chato (no bom
# sentido): repetível, auditável, sem credencial pessoal.
#
# **Como funciona**
#
# ```mermaid
# flowchart LR
#     PR["PR / push"] --> L["lint"] & T["test<br/>Java 17 + Spark"] & TF["terraform<br/>fmt + validate"]
#     L & T --> BV["bundle validate<br/>(só com host configurado)"]
#     L & T & TF & BV --> DD{"push na main?"} -->|sim| DEV["deploy dev"]
#     L & T & TF & BV --> DP{"tag v*?"} -->|sim| APR["aprovação<br/>ambiente prod"] --> PROD["deploy prod"]
# ```
#
# - **Autenticação por OIDC** (*workload identity federation*): o GitHub emite um token de curta duração para o
#   job; uma *federation policy* no service principal do Databricks aceita esse token. **Nenhum segredo de longa
#   duração** no repositório — só host e client id, que não são segredo.
# - **Aprovação de prod:** o ambiente `prod` do GitHub tem revisores obrigatórios; o job fica parado até alguém
#   aprovar.
# - **Release:** a tag precisa bater com a versão do `pyproject.toml` — o mesmo número no Git, no wheel e no job.

# %%
wf = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
gatilhos = wf.get(True) or wf.get("on")  # o YAML 1.1 lê a chave `on` como booleano True
print("gatilhos:", {k: v for k, v in gatilhos.items()})
print(f"\n{'job':<16} {'needs':<38} {'ambiente':<9} condição")
for nome, j in wf["jobs"].items():
    needs = j.get("needs", [])
    needs = ",".join(needs) if isinstance(needs, list) else needs
    print(f"{nome:<16} {needs or '-':<38} {j.get('environment', '-'):<9} {j.get('if', '-')[:70]}")

# %% [markdown]
# O passo "a tag tem de bater com a versão do pacote", executado aqui com duas tags hipotéticas:

# %%
versao_pkg = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
for tag in (f"v{versao_pkg}", "v9.9.9"):
    ok = tag == f"v{versao_pkg}"
    print(f"tag {tag:<8} × pyproject {versao_pkg} → {'deploy segue' if ok else 'deploy BARRADO'}")

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "No PR, o CI roda lint, testes com Spark e validação de Terraform e do bundle. Merge na
# > `main` faz deploy automático em dev; tag semântica faz deploy em prod depois de aprovação no ambiente do GitHub.
# > A autenticação é OIDC com service principal — nenhum token guardado — e prod roda como service principal,
# > não como pessoa."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Por que tag e não branch para prod:** a tag é imutável e nomeia exatamente o que foi para produção; rollback
#   = deploy da tag anterior. (Por isso o alvo `prod` não usa `git.branch: main`: no checkout de uma tag o HEAD
#   fica destacado e essa validação falharia.)
# - **`concurrency`:** em PR, push novo cancela o anterior; em `main`/tag nunca — deploy pela metade é pior.
# - **Teste de integração no Databricks:** depois do deploy em dev, `databricks bundle run -t dev medallion` com
#   dado de amostra fecha a lacuna de paridade (ADR 0005). Custa compute; roda na `main`, não em cada PR.
# - **Cache do Ivy no CI:** o JAR do Delta é baixado na 1ª sessão; com cache, o teste não depende do Maven Central.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - CI que leva 30 min mata o trunk-based; paralelize e mantenha a suíte de PR rápida.
# - Deploy automático em prod sem aprovação só com testes de integração e monitoramento maduros.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```bash
# # Autenticação local (OAuth no navegador; grava perfil em ~/.databrickscfg)
# databricks auth login --host https://adb-<id>.<n>.azuredatabricks.net
#
# databricks bundle validate -t dev                   # resolve variáveis e confere o esquema
# databricks bundle plan     -t dev                   # o que vai mudar
# databricks bundle deploy   -t dev                   # uv build + upload + cria "[dev <você>] oss_lakehouse_medallion"
# databricks bundle run      -t dev medallion         # executa e acompanha
# databricks bundle summary  -t dev                   # links dos recursos
# databricks bundle destroy  -t dev                   # remove sua cópia de dev
#
# # Prod: só pelo CI (tag v*), com OIDC e service principal
# databricks bundle deploy -t prod --var="service_principal_id=<application-id>"
# ```
#
# **Git folders** (antigo Repos) servem para desenvolver dentro do workspace com branch e commit pela UI; o deploy
# de produção continua sendo o bundle a partir do CI — nunca um job apontando para o Git folder de uma pessoa.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Trunk-based ou GitFlow para um time de dados? Por quê?**
# <details><summary>Resposta</summary>
# Trunk-based: branches curtas, PR pequeno, CI e deploy contínuo em dev. Branch longa diverge do schema e do dado de
# produção. GitFlow só para produtos com várias versões suportadas em paralelo.
# </details>
#
# **2. Rebase ou merge?**
# <details><summary>Resposta</summary>
# Rebase no que é local e não publicado (histórico limpo); nunca reescrever branch compartilhada. Na `main`, squash
# ou rebase merge para histórico linear; `--force-with-lease` se for inevitável reescrever.
# </details>
#
# **3. Um deploy quebrou produção. Revert ou reset?**
# <details><summary>Resposta</summary>
# `git revert` (commit novo, histórico preservado, CI e deploy rodam de novo). `reset` só local. E reverter o
# código não reverte a tabela: `RESTORE` do Delta para o dado, se necessário.
# </details>
#
# **4. Como você versiona um pacote e uma tabela?**
# <details><summary>Resposta</summary>
# Pacote: SemVer calculado de Conventional Commits, tag `vX.Y.Z`. Tabela: contrato versionado — coluna nova anulável
# é MINOR; remover/mudar tipo é MAJOR com aviso, convivência (view/tabela v2) e data de remoção.
# </details>
#
# **5. O que é um Declarative Automation Bundle (ex-Asset Bundle) e o que vai nele?**
# <details><summary>Resposta</summary>
# Definição em YAML dos recursos do Databricks junto com o código: `databricks.yml` (bundle, artefatos, variáveis,
# alvos) e `resources/*.yml` (jobs, pipelines). O CLI valida, constrói, sobe e cria/atualiza por alvo.
# </details>
#
# **6. Qual a diferença entre `mode: development` e `mode: production`?**
# <details><summary>Resposta</summary>
# Development: prefixo `[dev usuário]`, schedules pausados, concorrência liberada, sem lock. Production: valida
# caminhos não pessoais, exige `run_as`/permissões, impede sobrescrever cluster; `run_as` com service principal.
# </details>
#
# **7. Por que não declarar `pyspark` como dependência do wheel que vai para o Databricks?**
# <details><summary>Resposta</summary>
# O runtime já traz o Spark; o pip instalaria outro por cima e quebraria o cluster (ou mudaria a versão em
# silêncio). `pyspark`/`delta-spark` ficam em grupo de desenvolvimento local.
# </details>
#
# **8. Como o CI autentica no Databricks sem guardar token?**
# <details><summary>Resposta</summary>
# OIDC / workload identity federation: o GitHub emite token de curta duração; federation policy no service
# principal aceita; `DATABRICKS_AUTH_TYPE=github-oidc`, `id-token: write`.
# </details>
#
# **9. Como fica a pirâmide de testes num pipeline de dados?**
# <details><summary>Resposta</summary>
# Muitos unitários (funções e transformações com chispa), alguns de integração com Spark local e Delta
# (idempotência), testes de dados/contrato em produção a cada lote, e um ponta a ponta no ambiente dev.
# </details>
#
# **10. Por que este repositório versiona as saídas dos notebooks e não usa nbstripout?**
# <details><summary>Resposta</summary>
# As saídas são o produto (evidência legível no GitHub); a fonte revisável é o `.py` jupytext e o `.ipynb` é gerado
# e executado por script. Em produção, o padrão é nbstripout para não vazar dado nem poluir o diff.
# </details>
#
# **11. Retries: em quais tasks e quantos?**
# <details><summary>Resposta</summary>
# Só em tasks idempotentes (checkpoint, MERGE), poucos e com intervalo — cobrem falha transitória. Task de
# qualidade não tem retry: reprovação é sinal. Junto, `max_concurrent_runs: 1` evita execuções sobrepostas.
# </details>
#
# **12. Como você protege a `main`?**
# <details><summary>Resposta</summary>
# Ruleset: PR obrigatório com aprovação e CODEOWNERS, checks do CI obrigatórios, histórico linear, sem force push e
# sem exclusão. pre-commit local para o barato; o CI repete porque o gancho local é opcional.
# </details>

# %% [markdown]
# ## Resumo
#
# - Trunk-based + PR pequeno + CI rápido; rebase no local, revert no compartilhado, reflog salva.
# - Conventional Commits → SemVer automático; contrato de tabela também tem MAJOR/MINOR/PATCH.
# - Wheel com `uv build`, entry point registrado, **sem `pyspark` nas dependências** do que vai ao cluster.
# - Bundle: `dev` (prefixo, agenda pausada) e `prod` (service principal, permissões); job bronze → silver → gold →
#   quality com retries só onde é idempotente.
# - GitHub Actions: lint, testes com Spark, Terraform, bundle; deploy dev na `main`, prod por tag com aprovação, OIDC.
