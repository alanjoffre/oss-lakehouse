# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 14 · Azure com Terraform + Azurite (custo zero)
#
# > A plataforma Azure do lakehouse — ADLS Gen2, Access Connector, Key Vault, Azure Databricks com Unity Catalog —
# > descrita em Terraform **validado**, e o caminho do dado até o Blob Storage exercitado de verdade, offline, no
# > emulador Azurite: SDK, SAS de menor privilégio e Spark gravando **Delta via `abfs://`**.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Microsoft Azure | ADLS Gen2 (HNS), Access Connector + identidade gerenciada, RBAC, Key Vault, rede, custos |
# | Databricks | workspace premium, Unity Catalog (storage credential, external location, catálogo, grants), cluster policy |
# | Git/versionamento | infraestrutura como código, versões de provider fixadas, estado remoto, ambientes dev/prod |
# | Arquitetura de pipelines | onde cada camada vive, quem acessa o quê, quanto custa |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Honestidade de escopo.** Não há assinatura Azure aqui: `terraform validate` roda; `terraform plan` **não**
# (precisa autenticar na Azure — mostramos a falha). O Azurite emula o **Blob Storage**, não o ADLS Gen2 com
# namespace hierárquico.

# %% [markdown]
# ## Setup

# %%
import json
import os
import re
import shutil
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

from oss_lakehouse.config import PROJECT_ROOT, get_settings
from oss_lakehouse.spark import get_spark

s = get_settings()
TF_DIR = PROJECT_ROOT / "infra" / "terraform" / "azure"
COMPOSE = PROJECT_ROOT / "infra" / "azurite" / "docker-compose.yml"
DEMO = Path(s.data_root) / "demo" / "14"
JARS = DEMO / "_jars"  # cache dos JARs baixados (sobrevive entre execuções)
for p in DEMO.glob("*"):
    if p != JARS:
        shutil.rmtree(p) if p.is_dir() else p.unlink()
JARS.mkdir(parents=True, exist_ok=True)

TERRAFORM = shutil.which("terraform") or str(Path.home() / ".local" / "bin" / "terraform")
DOCKER = shutil.which("docker")
SPARK_JARS = Path(__import__("pyspark").__file__).parent / "jars"
HADOOP = re.search(r"hadoop-client-api-([\d.]+)\.jar", " ".join(p.name for p in SPARK_JARS.iterdir())).group(1)
print(f"terraform: {Path(TERRAFORM).exists()} | docker: {bool(DOCKER)} | Hadoop dentro do Spark: {HADOOP}")


def sh(cmd, cwd=TF_DIR, timeout=300):
    """Roda um comando e devolve (código, saída sem cores)."""
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout,
                       env={**os.environ, "TF_IN_AUTOMATION": "1", "NO_COLOR": "1"})
    return r.returncode, re.sub(r"\x1b\[[0-9;]*m", "", r.stdout + r.stderr)


# %% [markdown]
# ## 1. A arquitetura na Azure
#
# **O que é.** O desenho mínimo de um lakehouse Databricks na Azure, todo declarado em `infra/terraform/azure/`:
#
# ```text
#  Resource Group rg-osslh-<env>                                   (tags: project, environment, cost_center, owner)
#  ├── Storage Account stosslh<env>lake  — ADLS Gen2 (is_hns_enabled = true), sem chave de conta, firewall Deny
#  │     ├── container lake/        landing/ bronze/ silver/ gold/      ← external location do UC
#  │     └── container uc-managed/  storage GERENCIADO do catálogo      ← managed location do UC
#  ├── Access Connector dbac-osslh-<env> (identidade gerenciada)
#  │     └── role "Storage Blob Data Contributor" NA conta acima (menor privilégio)
#  ├── Key Vault kv-osslh-<env> (modo RBAC, soft delete, purge protection em prod)
#  └── Azure Databricks dbw-osslh-<env> (SKU premium, no_public_ip)
#        └── Unity Catalog: storage credential → external locations → catálogo oss_lakehouse_<env>
#              schemas bronze/silver/gold · grants por grupo · cluster policy · secret scope "kv"
# ```
#
# **Por que importa.** Em entrevista de sênior a pergunta não é "você sabe criar um cluster?", e sim "quem acessa
# o storage, com qual identidade, por qual rede, e quanto custa?". Cada caixa acima responde uma dessas.
#
# **Como funciona.** Os recursos declarados, lidos dos próprios `.tf`:

# %%
resources = []
for tf in sorted(TF_DIR.glob("*.tf")):
    resources += [(tf.name, t, n) for t, n in re.findall(r'^resource "([\w]+)" "([\w]+)"', tf.read_text(), re.M)]
for f, t, n in resources:
    print(f"{f:26s} {t}.{n}")
print(len(resources), "recursos em", len({f for f, _, _ in resources}), "arquivos")

# %% [markdown]
# **Um container `lake` com pastas por camada — por quê.** O código já usa `data_root = abfss://lake@<conta>...`
# e `path("silver", ...)`: um container mantém o mesmo layout do laptop. Com Unity Catalog, a permissão não é mais
# dada por container (ACL/RBAC no storage) e sim por **external location**/tabela no catálogo — separar camadas em
# containers deixou de ser necessário para segurança. O que **é** separado: o storage **gerenciado** do catálogo
# (`uc-managed`), porque o UC não permite sobrepor caminho de tabela externa e de tabela gerenciada; e **ambientes**
# (dev/prod) em contas diferentes — raio de impacto (*blast radius*) e custo separados.
#
# > 🎤 **Resposta de 30 s:** "Na Azure, o lakehouse é: ADLS Gen2 com namespace hierárquico, acessado pelo Unity
# > Catalog através de um Access Connector com identidade gerenciada — sem segredo; Key Vault para o que ainda é
# > segredo; workspace premium por causa do UC; tudo em Terraform, um estado por ambiente, e tags em tudo para
# > separar custo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Metastore do UC é regional** e, para workspaces novos, criado/atribuído automaticamente; por isso o
#   Terraform não cria metastore — só os objetos dentro dele.
# - **Container por camada** ainda faz sentido quando consumidores **fora** do Databricks (Synapse, Fabric, ADF)
#   acessam o storage direto e precisam de RBAC por container.
# - **Duas stacks**: o provider `databricks` aponta para o workspace criado no mesmo apply — funciona, mas o
#   recomendado é separar "infra Azure" de "objetos do Databricks" em estados diferentes (ciclos de vida
#   diferentes, menos acoplamento).
# </details>
#
# **Trade-offs**
# - Uma conta de storage por ambiente simplifica governança mas multiplica private endpoints (custo por hora).
# - Separar demais (conta por camada) aumenta custo fixo e complexidade de rede sem ganho real com UC.

# %% [markdown]
# ## 2. Terraform: fmt, validate — e por que `plan` não roda aqui
#
# **O que é.** **IaC** (*Infrastructure as Code*): a infraestrutura descrita em arquivo versionado, revisada em PR e
# aplicada por pipeline. O Terraform compara o desejado (código) com o **estado** (o que ele criou) e com a nuvem.
#
# **Por que importa.** Ambiente clicado no portal não se reproduz, não se revisa e deriva. Com IaC, dev e prod são
# o mesmo código com `tfvars` diferentes.
#
# **Como funciona.** `fmt -check` (estilo), `init -backend=false` (baixa os providers **nas versões fixadas**, sem
# estado remoto) e `validate` (sintaxe + schema dos providers, sem chamar a Azure). É exatamente o que o
# `make tf-validate` e o CI rodam. O `init` precisa de internet (registry dos providers) na primeira vez; depois,
# `fmt` e `validate` rodam offline.

# %%
t0 = time.time()
if Path(TERRAFORM).exists():
    rc_init, out_init = sh([TERRAFORM, "init", "-backend=false", "-input=false"])
    rc_fmt, out_fmt = sh([TERRAFORM, "fmt", "-check", "-recursive"])
    rc_val, out_val = sh([TERRAFORM, "validate", "-no-color"])
    if rc_init == 0:
        print("init: ok")
    elif (TF_DIR / ".terraform" / "providers").exists():
        # `init` consulta o registry.terraform.io mesmo com os providers já baixados; sem rede ele falha,
        # mas o `validate` continua funcionando com o que está em .terraform/.
        print("init: sem acesso ao registry (offline) — usando os providers já instalados em .terraform/")
    else:
        print("init falhou e não há providers instalados:", out_init.strip().splitlines()[-1][:200])
    print("fmt -check:", "ok (nada a formatar)" if rc_fmt == 0 else out_fmt)
    print("validate:", out_val.strip())
    _, ver = sh([TERRAFORM, "version", "-json"])
    v = json.loads(ver)
    print("terraform", v["terraform_version"], "| providers:", v.get("provider_selections"))
    print(f"({time.time() - t0:.0f} s)")
else:
    print("terraform não instalado — veja `make tf-validate` no README")

# %% [markdown]
# Agora o `plan`, com o `dev.tfvars.example`. Ele precisa de credencial da Azure (Azure CLI logado, service principal
# ou identidade federada do CI) para ler o estado real da assinatura — aqui não há nenhuma:

# %%
if Path(TERRAFORM).exists():
    rc, out = sh([TERRAFORM, "plan", "-input=false", "-lock=false", "-refresh=false",
                  "-var-file=envs/dev.tfvars.example"], timeout=180)
    errors = [ln.strip() for ln in out.splitlines() if ln.strip().startswith(("Error", "│ Error"))]
    print(f"terraform plan → código {rc}")
    print("\n".join(errors[:3]) or out[-600:])

# %% [markdown]
# Falha esperada e honesta: sem autenticar, o provider `azurerm` não consegue nem montar o cliente da assinatura.
# Num pipeline real, o CI autentica por **OIDC** (identidade federada do GitHub Actions no Entra ID — sem segredo
# guardado), roda `plan`, publica o plano no PR e só aplica depois da aprovação.
#
# > 🎤 **Resposta de 30 s:** "Terraform com versões de provider fixadas, um estado remoto por ambiente com lock, e
# > o mesmo código para dev e prod mudando só o tfvars. No PR roda fmt, validate e plan; o apply é do pipeline,
# > autenticado por OIDC — ninguém aplica do laptop."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Estado** contém segredos e IDs: fica num Storage Account dedicado (backend `azurerm`, ver `backend.tf`), com
#   lock por *lease* de blob, versionamento e acesso restrito.
# - **Pin de versão**: o `azurerm` 4 → 5 mudou atributos (ex.: `enable_rbac_authorization` virou
#   `rbac_authorization_enabled`); sem pin, um `init` numa máquina nova muda o comportamento.
#   (`.terraform.lock.hcl` também fixa os hashes — este repositório o ignora no Git; em time, versione-o.)
# - **Drift** (alguém mudou no portal): `plan` agendado detecta; política (Azure Policy) previne.
# - Alternativas: Bicep (nativo Azure, sem estado próprio), Pulumi (linguagem de programação). Terraform ganha por
#   cobrir Azure **e** Databricks no mesmo fluxo.
# </details>
#
# **Trade-offs**
# - Provider do Databricks configurado a partir do workspace recém-criado: no primeiro apply pode exigir dois passos.
# - `validate` não pega erro semântico de nuvem (nome já usado, cota de vCPU, região sem SKU) — só o `plan`/`apply`.

# %% [markdown]
# ## 3. Identidade: managed identity × service principal × SAS × chave da conta
#
# **O que é.** Quatro jeitos de um processo provar quem é para o Storage:
#
# | Mecanismo | O que é | Segredo para guardar? | Escopo | Quando usar |
# |---|---|---|---|---|
# | **Chave da conta** (*account key*) | senha mestra da conta | sim, e vale **tudo** | conta inteira, sem auditoria por usuário | nunca em produção (aqui: `shared_access_key_enabled = false`) |
# | **SAS** (*shared access signature*) | URL assinada com permissão e validade | o token em si | container/blob, permissões e prazo | compartilhar um arquivo com terceiro por tempo curto; prefira *user delegation SAS* (assinada via Entra ID) |
# | **Service principal** | identidade de aplicação no Entra ID | client secret ou certificado (rotacionar!) | o que o RBAC der | sistemas fora da Azure; legado de Databricks sem UC |
# | **Managed identity** | identidade gerenciada pela Azure, presa a um recurso | **não** | o que o RBAC der | tudo que roda na Azure — aqui, o **Access Connector** do UC |
#
# **Por que importa.** Segredo que não existe não vaza. Com identidade gerenciada, não há o que rotacionar nem o que
# esquecer num notebook.
#
# **Como funciona.** O Unity Catalog guarda uma **storage credential** que aponta para o Access Connector; a
# identidade dele tem o papel *Storage Blob Data Contributor* na conta. O usuário nunca vê credencial: o UC checa o
# grant e acessa o storage em nome dele. Abaixo, no Azurite, a diferença prática entre chave e SAS de leitura.

# %% [markdown]
# ## 4. Azurite: o Blob Storage local
#
# **O que é.** O **Azurite** é o emulador oficial do Azure Storage (Blob, Queue, Table), em Docker. Usa uma conta
# de desenvolvimento **pública e documentada** (`devstoreaccount1`).
#
# **Por que importa.** Testar código que fala com storage — upload, listagem, SAS, Spark — sem assinatura, sem custo
# e no CI.
#
# **Como funciona.** `docker compose` sobe o emulador na porta 10000; o SDK `azure-storage-blob` conecta por
# *connection string*.

# %%
# Conta de DESENVOLVIMENTO do Azurite — pública na documentação da Microsoft, não é segredo.
ACCOUNT = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
BLOB_URL = f"http://127.0.0.1:10000/{ACCOUNT}"
CONN = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};BlobEndpoint={BLOB_URL};"

AZURITE = False
if DOCKER:
    rc, out = sh([DOCKER, "compose", "-f", str(COMPOSE), "up", "-d", "--wait"], cwd=PROJECT_ROOT, timeout=240)
    AZURITE = rc == 0
    print("azurite:", "no ar" if AZURITE else out[-400:])
else:
    print("docker indisponível: as seções 4 e 5 só explicam (o Terraform acima não depende disso)")


def azurite_down():
    """Para o emulador. Chamado no fim e também se a seção do Spark falhar — nada fica rodando."""
    rc, out = sh([DOCKER, "compose", "-f", str(COMPOSE), "down"], cwd=PROJECT_ROOT, timeout=120)
    return "azurite parado" if rc == 0 else out[-300:]

# %%
if AZURITE:
    from azure.core.exceptions import HttpResponseError, ResourceExistsError
    from azure.storage.blob import (
        BlobServiceClient,
        ContainerClient,
        ContainerSasPermissions,
        generate_container_sas,
    )

    svc = BlobServiceClient.from_connection_string(CONN)
    for c in ("lake", "uc-managed"):
        try:
            svc.create_container(c)
        except ResourceExistsError:
            pass
    info = svc.get_account_information()
    print("containers:", [c.name for c in svc.list_containers()])
    print("conta:", info["account_kind"], "| namespace hierárquico (HNS):", info["is_hns_enabled"])

    # Sobe a bronze inteira (tabela Delta = parquet + _delta_log) para lake/bronze/gh_events.
    src = Path(s.path("bronze", "gh_events"))
    lake = svc.get_container_client("lake")
    t0, n, size = time.time(), 0, 0
    for f in sorted(src.rglob("*")):
        if f.is_file() and not f.name.endswith(".crc"):
            with f.open("rb") as fh:
                lake.upload_blob(f"bronze/gh_events/{f.relative_to(src)}", fh, overwrite=True)
            n, size = n + 1, size + f.stat().st_size
    print(f"upload: {n} arquivos, {size / 1e6:.1f} MB em {time.time() - t0:.1f} s")
    for b in list(lake.walk_blobs("bronze/gh_events/", delimiter="/")):
        print("  ", b.name)

# %% [markdown]
# Repare em `is_hns_enabled: False`: o Azurite é uma conta **sem** namespace hierárquico (*flat namespace*, FNS).
# "Pastas" ali são prefixos de nome; renomear uma pasta = copiar e apagar cada blob. No ADLS Gen2 real (HNS), rename
# de diretório é atômico — o que importa para quem confia em rename no commit de jobs.
#
# **SAS de menor privilégio.** Uma SAS de **leitura + listagem** no container, válida por 1 hora: lista e lê, mas não
# escreve.

# %%
if AZURITE:
    sas = generate_container_sas(ACCOUNT, "lake", account_key=KEY,
                                 permission=ContainerSasPermissions(read=True, list=True),
                                 expiry=datetime.now(UTC) + timedelta(hours=1))
    print("token SAS (parâmetros):", sorted({kv.split("=")[0] for kv in sas.split("&")}))
    ro = ContainerClient.from_container_url(f"{BLOB_URL}/lake?{sas}")
    print("listar com a SAS:", sum(1 for _ in ro.list_blobs(name_starts_with="bronze/")), "blobs")
    try:
        ro.upload_blob("bronze/invasor.txt", b"x")
    except HttpResponseError as exc:
        print("escrever com a SAS:", exc.status_code, exc.error_code)

# %% [markdown]
# > 🎤 **Resposta de 30 s:** "Prefiro identidade gerenciada a qualquer segredo: no Databricks com UC, o Access
# > Connector acessa o storage e eu dou grant no catálogo. SAS só para compartilhar algo pontual, com escopo mínimo
# > e prazo curto — de preferência user delegation SAS. Chave da conta eu desligo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - `sp` = permissões, `se` = expiração, `sig` = assinatura HMAC com a chave — quem tem a chave fabrica qualquer SAS;
#   por isso *user delegation SAS* (assinada com credencial Entra ID, revogável) é a forma recomendada.
# - SAS de conta (*account SAS*) × de serviço (container/blob): a de serviço limita o escopo.
# - **Data plane × control plane**: `Contributor` na conta não lê dado (controle); `Storage Blob Data Reader/
#   Contributor` lê/escreve dado. Menor privilégio = papel de dados, no escopo mínimo.
# </details>
#
# **Trade-offs**
# - Azurite não valida RBAC/Entra ID nem firewall: testa o código, não a política de acesso.
# - Emulador fica atrás da nuvem em versões de API (`--skipApiVersionCheck` no compose).

# %% [markdown]
# ## 5. Spark lendo e escrevendo no Azurite: WASB morreu, ABFS sobre Blob funciona
#
# **O que é.** O Spark fala com a Azure pelos drivers do Hadoop (`hadoop-azure`):
# - **WASB** (`wasb://`): driver antigo, sobre o endpoint Blob;
# - **ABFS** (`abfs://`, `abfss://` com TLS): driver do ADLS Gen2, sobre o endpoint **DFS**
#   (`<conta>.dfs.core.windows.net`). É o que o Databricks usa.
#
# **Por que importa.** O caminho `abfss://lake@<conta>.dfs.core.windows.net/silver/...` é o `data_root` de produção.
# Exercitar o mesmo driver localmente pega erro de configuração antes da nuvem.
#
# **Como funciona.** O Spark 4.2 traz o **Hadoop 3.5**. Baixamos o `hadoop-azure` da mesma versão (o JAR tem de casar
# com o Hadoop do Spark) e passamos via `get_spark(**extra_conf)` com `spark.jars`. Primeiro, a tentativa com WASB.

# %%
def fetch_jar(group, artifact, version):
    path = JARS / f"{artifact}-{version}.jar"
    if not path.exists():
        url = f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{path.name}"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        path.write_bytes(r.content)
    return str(path)


try:
    jars = [fetch_jar("org.apache.hadoop", "hadoop-azure", HADOOP),
            fetch_jar("org.wildfly.openssl", "wildfly-openssl", "2.2.5.Final")]  # dependência do ABFS
except requests.RequestException as exc:
    jars = []
    print(f"sem rede e sem cache dos JARs ({type(exc).__name__}): Spark × Azurite fica só explicado")

ABFS_AZURITE = {
    "spark.jars": ",".join(jars),
    # Endpoint do emulador no lugar de <conta>.dfs.core.windows.net (com IP:porta o ABFS põe a conta no caminho).
    "spark.hadoop.fs.azure.abfs.endpoint": "127.0.0.1:10000",
    "spark.hadoop.fs.azure.always.use.https": "false",
    "spark.hadoop.fs.azure.account.auth.type": "SharedKey",
    f"spark.hadoop.fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net": KEY,
    # Conta sem HNS (o Azurite): ABFS no modo FNS falando com o endpoint Blob — novidade do Hadoop 3.5.
    "spark.hadoop.fs.azure.account.hns.enabled": "false",
    "spark.hadoop.fs.azure.fns.account.service.type": "BLOB",
    # O cliente HTTP padrão (Apache) exige TLS; o emulador é HTTP puro.
    "spark.hadoop.fs.azure.networking.library": "JDK_HTTP_URL_CONNECTION",
}
spark = get_spark("14", **(ABFS_AZURITE if jars and AZURITE else {}))
SPARK_AZ = bool(jars and AZURITE)

# %%
if SPARK_AZ:
    try:
        spark.read.text(f"wasb://lake@{ACCOUNT}.blob.core.windows.net/bronze/").count()
    except Exception as exc:  # Py4JJavaError
        msg = str(exc)
        print("wasb:// →", re.search(r"(WASB Driver[^.]*\.)", msg).group(1) if "WASB Driver" in msg else msg[:300])

# %% [markdown]
# A própria mensagem do Hadoop 3.5: o driver WASB foi **removido** (sobrou um *stub* que só lança erro) e a orientação
# é usar ABFS também para contas sem HNS. Então, ABFS: lemos como **Delta** a bronze que o SDK subiu e gravamos uma
# agregação de volta.

# %%
if SPARK_AZ:
    try:
        base = f"abfs://lake@{ACCOUNT}.dfs.core.windows.net"
        t0 = time.time()
        remote = spark.read.format("delta").load(f"{base}/bronze/gh_events")
        local_n = spark.read.format("delta").load(s.path("bronze", "gh_events")).count()
        print(f"bronze via abfs://: {remote.count()} linhas (local: {local_n}) em {time.time() - t0:.1f} s")
        by_type = remote.groupBy("type").count()
        by_type.write.format("delta").mode("overwrite").save(f"{base}/silver/eventos_por_tipo")
        back = spark.read.format("delta").load(f"{base}/silver/eventos_por_tipo")
        back.orderBy("count", ascending=False).show(3)
        hist = spark.sql(f"DESCRIBE HISTORY delta.`{base}/silver/eventos_por_tipo`").select("version", "operation")
        print("histórico Delta no Azurite:", [tuple(r) for r in hist.collect()])
        print("blobs gravados pelo Spark (vistos pelo SDK):")
        for b in lake.list_blobs(name_starts_with="silver/eventos_por_tipo/"):
            print(f"   {b.name}  ({b.size} bytes)")
    except Exception:
        print(azurite_down(), "(a seção falhou; o erro segue abaixo)")
        raise

# %% [markdown]
# Funciona de ponta a ponta: o SDK sobe, o Spark lê a tabela Delta pelo ABFS, agrega, grava um Delta novo, e o SDK
# enxerga os blobs (`_delta_log/…json` + `part-….parquet`). Os blobs de **0 bytes** com nome de pasta são marcadores
# que o ABFS cria para simular diretório numa conta sem HNS — no ADLS Gen2 o diretório existe de verdade.
# O que **não** foi testado aqui, por limitação do emulador:
# - `abfss://` (TLS) — o Azurite fala HTTP; em produção é sempre `abfss`;
# - o endpoint **DFS** com HNS (rename atômico de diretório, ACLs POSIX) — o Azurite não emula ADLS Gen2;
# - autenticação OAuth/identidade gerenciada — aqui é a chave pública do emulador.
#
# > 🎤 **Resposta de 30 s:** "No Databricks o caminho é `abfss://container@conta.dfs.core.windows.net`, driver ABFS,
# > identidade do Unity Catalog. Localmente eu exercito o mesmo driver contra o Azurite — no Hadoop 3.5 o WASB foi
# > removido e o ABFS fala com conta sem HNS pelo endpoint Blob. Testo o código; a política de acesso, só na nuvem."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Delta em object storage**: o commit do Delta usa "put-if-absent" do arquivo de log; no ADLS isso é atômico,
#   por isso múltiplos escritores são seguros. (Em S3 era preciso um *LogStore* com DynamoDB até o S3 ganhar
#   escrita condicional.)
# - O `get_spark` do projeto usa `configure_spark_with_delta_pip`, que **sobrescreve** `spark.jars.packages`; por isso
#   aqui os JARs vão por `spark.jars` (arquivos locais) — e a versão vem do JAR do Hadoop embutido no Spark.
# - Sem UC (legado), o acesso por service principal era configurado assim — e o segredo vinha do secret scope:
#   ```python
#   spark.conf.set("fs.azure.account.auth.type.<conta>.dfs.core.windows.net", "OAuth")
#   spark.conf.set("fs.azure.account.oauth.provider.type.<conta>.dfs.core.windows.net",
#                  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#   spark.conf.set("fs.azure.account.oauth2.client.secret.<conta>.dfs.core.windows.net",
#                  dbutils.secrets.get("kv", "sp-secret"))
#   ```
#   Com UC, nada disso: storage credential + external location + grant.
# </details>
#
# **Trade-offs**
# - Emulador dá confiança no código e no formato, não em desempenho, rede ou permissão.
# - Teste de integração contra uma conta real barata (LRS, dev, apagada no fim) complementa o Azurite no CI.

# %% [markdown]
# ## 6. Rede, segredos e custos
#
# **O que é.**
# - **VNet injection**: o plano de dados do Databricks (VMs dos clusters) roda numa VNet **sua**, com sub-redes
#   pública/privada — controla rotas, NSG, firewall de saída. `no_public_ip = true` (*secure cluster connectivity*):
#   os nós não têm IP público.
# - **Private endpoint**: o Storage, o Key Vault e o próprio workspace ganham IP privado na VNet; com
#   `public_network_access = "Disabled"` (prod), não há caminho pela internet.
# - **Secret scope apoiado no Key Vault**: `dbutils.secrets.get("kv", "github-token")` lê do cofre; o valor sai como
#   `[REDACTED]` em notebook e log.
#
# **Por que importa.** Dado sensível com caminho público é achado de auditoria. E custo de nuvem sem tag é custo
# sem dono.
#
# **Como funciona.** O que o Terraform já expressa, conferido no código:

# %%
checks = {
    "HNS ligado (ADLS Gen2)": r"is_hns_enabled\s*=\s*true",
    "chave da conta desligada": r"shared_access_key_enabled\s*=\s*false",
    "firewall do storage nega por padrão": r'default_action\s*=\s*"Deny"',
    "Key Vault em modo RBAC": r"rbac_authorization_enabled\s*=\s*true",
    "workspace premium": r'sku\s*=\s*"premium"',
    "nós sem IP público": r"no_public_ip\s*=\s*true",
    "autodesligamento na cluster policy": r"autotermination_minutes",
    "tags de custo em tudo": r"tags\s*=\s*local\.tags",
}
code = "\n".join(p.read_text() for p in TF_DIR.glob("*.tf"))
for label, pattern in checks.items():
    print(f"{'✔' if re.search(pattern, code) else '✘'} {label} ({len(re.findall(pattern, code))}×)")

# %% [markdown]
# **Onde ficam os custos** (o que entra na fatura de um lakehouse Databricks na Azure):
#
# | Item | Cobra por | Como controlar |
# |---|---|---|
# | **DBU** (Databricks) | DBU-hora × tipo de compute (jobs < all-purpose; serverless e SQL warehouse à parte) | job compute em vez de all-purpose; cluster policy; autodesligamento; serverless para picos |
# | **VMs** dos clusters (clássico) | hora de VM no RG gerenciado | tamanho/limite na policy; *spot* com fallback; tags herdadas |
# | **Storage** | GB-mês + **transações** | *lifecycle* (hot → cool → archive) na landing; menos *small files* = menos transações |
# | **Rede** | private endpoint (hora + GB), NAT Gateway, saída para internet | um endpoint por serviço necessário; evitar tráfego entre regiões |
# | **Key Vault / Access Connector** | operações / grátis | desprezível |
#
# O workspace em si não cobra; o que cobra é compute e o que ele toca. As tags `custom_tags` da cluster policy vão
# para as VMs e para o uso de DBU — é assim que o Cost Management e as *system tables* de billing separam por
# projeto (notebook 15).
#
# > 🎤 **Resposta de 30 s:** "Rede: VNet injection, nós sem IP público, private endpoint para storage e Key Vault,
# > acesso público desligado em prod. Segredo: Key Vault com secret scope, e o que der vira identidade gerenciada.
# > Custo: DBU e VM são o grosso — cluster policy com limite, autodesligamento e tags obrigatórias."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - Serverless compute roda no plano de dados **da Databricks**: o acesso ao seu storage privado passa por
#   *network connectivity configuration* (NCC) e private endpoints gerenciados — outro desenho de rede.
# - Front-end privado (Private Link para o workspace) × back-end privado (plano de dados ↔ plano de controle):
#   são configurações diferentes.
# - Secret scope do Key Vault: o cofre precisa permitir o app "AzureDatabricks" (papel *Key Vault Secrets User* em
#   modo RBAC) e "serviços confiáveis" no firewall; criar o scope exige login de usuário Entra ID.
# </details>
#
# **Trade-offs**
# - Private endpoints custam por hora cada; em dev, firewall com IP liberado costuma bastar.
# - VNet injection exige planejar faixas de IP com folga: cada nó consome IPs das duas sub-redes.

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```bash
# # 1) Infra (pipeline de CI autenticado por OIDC)
# cd infra/terraform/azure
# terraform init -backend-config="key=oss-lakehouse/dev.tfstate"
# terraform plan  -var-file=envs/dev.tfvars -out=tfplan
# terraform apply tfplan
#
# # 2) Segredo no Key Vault (quem aplica tem "Key Vault Secrets Officer")
# az keyvault secret set --vault-name kv-osslh-dev --name github-token --value "$GITHUB_TOKEN"
#
# # 3) Secret scope apoiado no Key Vault (Databricks CLI, login de usuário Entra ID)
# databricks secrets create-scope --json '{
#   "scope": "kv", "scope_backend_type": "AZURE_KEYVAULT",
#   "backend_azure_keyvault": {"resource_id": "<id do key vault>", "dns_name": "https://kv-osslh-dev.vault.azure.net/"}}'
# ```
#
# ```python
# # 4) No notebook/job: mesmo código do laptop, outra raiz
# import os
# os.environ["OSSLH_ENV"] = "databricks"
# os.environ["OSSLH_DATA_ROOT"] = "abfss://lake@stosslhdevlake.dfs.core.windows.net"   # output data_root
# token = dbutils.secrets.get("kv", "github-token")                                     # [REDACTED] se impresso
# spark.sql("USE CATALOG oss_lakehouse_dev")
# spark.sql("CREATE TABLE IF NOT EXISTS bronze.gh_events_ext LOCATION "
#           "'abfss://lake@stosslhdevlake.dfs.core.windows.net/bronze/gh_events'")     # external location cobre
# ```

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Por que ADLS Gen2 e não Blob Storage comum para o lakehouse?**
# <details><summary>Resposta</summary>
# O namespace hierárquico dá diretórios reais (rename/delete de pasta atômicos e rápidos), ACLs POSIX e o endpoint DFS
# do driver ABFS. Para Spark/Delta isso significa operações de diretório O(1) em vez de copiar blob a blob.
# </details>
#
# **2. Como o Databricks acessa o storage sem segredo?**
# <details><summary>Resposta</summary>
# Access Connector com identidade gerenciada + papel Storage Blob Data Contributor na conta; no Unity Catalog, uma
# storage credential aponta para o connector e external locations definem os caminhos. Usuários recebem grants no
# catálogo, nunca credenciais.
# </details>
#
# **3. Managed identity × service principal × SAS × account key?**
# <details><summary>Resposta</summary>
# Managed identity: sem segredo, para recursos na Azure (preferida). Service principal: identidade de app com segredo
# a rotacionar, para quem está fora. SAS: acesso delegado com escopo e prazo, para compartilhamento pontual. Account
# key: acesso total sem rastreio por usuário — desligar.
# </details>
#
# **4. Por que o workspace precisa ser premium?**
# <details><summary>Resposta</summary>
# Unity Catalog com controle de acesso fino, cluster policies, audit logs e outros recursos de governança exigem o
# tier premium.
# </details>
#
# **5. Como você organiza o Terraform para dev e prod?**
# <details><summary>Resposta</summary>
# Mesmo código, tfvars por ambiente, estado remoto separado por ambiente (backend azurerm com lock), versões de provider
# fixadas, plan no PR e apply pelo pipeline autenticado por OIDC. Idealmente assinaturas separadas.
# </details>
#
# **6. Onde ficam os segredos e como o notebook os lê?**
# <details><summary>Resposta</summary>
# No Key Vault (modo RBAC); o Databricks lê por secret scope apoiado no cofre com <code>dbutils.secrets.get</code>, e o valor
# aparece mascarado. O ideal é eliminar segredos trocando por identidade gerenciada sempre que possível.
# </details>
#
# **7. O que é VNet injection e private endpoint, e quando vale o custo?**
# <details><summary>Resposta</summary>
# VNet injection põe os clusters numa VNet sua (controle de rota, NSG, firewall de saída). Private endpoint dá IP
# privado ao storage/Key Vault/workspace e permite desligar o acesso público. Vale em produção com dado sensível ou
# exigência regulatória; em dev, firewall por IP costuma bastar.
# </details>
#
# **8. Para que serve o Azurite e qual o limite dele?**
# <details><summary>Resposta</summary>
# Emular Blob Storage localmente para testar código (SDK, SAS, Spark via ABFS em modo FNS) sem custo. Não emula ADLS
# Gen2 (HNS/DFS), TLS do abfss, RBAC/Entra ID nem rede — política de acesso se testa na nuvem.
# </details>
#
# **9. Quais são os maiores custos de um lakehouse Databricks na Azure e como controlar?**
# <details><summary>Resposta</summary>
# DBU e VMs de compute. Controle: job compute em vez de all-purpose, cluster policy com limites e autodesligamento,
# spot, serverless para cargas intermitentes, tags obrigatórias e acompanhamento por system tables de billing.
# Storage pesa pouco, mas small files aumentam transações.
# </details>
#
# **10. Um container por camada ou um container com pastas?**
# <details><summary>Resposta</summary>
# Com Unity Catalog, a permissão é no catálogo/external location, então um container com pastas basta e mantém o
# layout do código; separo o storage gerenciado do catálogo e os ambientes (contas diferentes). Container por camada
# volta a fazer sentido se ferramentas fora do Databricks acessam o storage direto.
# </details>

# %% [markdown]
# ## Resumo
#
# - Arquitetura: ADLS Gen2 (HNS) + Access Connector (identidade gerenciada, papel de dados de menor privilégio) +
#   Key Vault RBAC + workspace premium com Unity Catalog — tudo em Terraform validado, versões fixadas.
# - Sem segredo é melhor que segredo bem guardado: managed identity > service principal > SAS; chave da conta desligada.
# - `plan` precisa de credencial (no CI, OIDC); `validate` e `fmt` rodam em qualquer lugar.
# - Azurite testa o código de storage de graça; no Hadoop 3.5 (Spark 4.2) o WASB saiu e o ABFS fala com conta FNS —
#   provado aqui com Delta lido e gravado.
# - Custo: DBU e VM dominam; policy, autodesligamento e tags são os freios.

# %%
spark.stop()
if AZURITE:
    print(azurite_down())
