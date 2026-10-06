# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 11 · Governança, Unity Catalog e LGPD
#
# > Este notebook prova três coisas: (1) "fazer hash do login" não protege ninguém — o ataque roda aqui em
# > segundos; (2) `DELETE` numa tabela Delta não apaga o dado do disco — o time travel devolve, e só some depois
# > de `VACUUM` (e de `REORG ... PURGE`, se houver deletion vectors); (3) o que o Unity Catalog faz por política
# > declarativa e o que continua sendo trabalho do pipeline.
#
# | Competência | Onde aparece aqui |
# |---|---|
# | Databricks e processamento de dados | §1–§3 (Unity Catalog, managed × external, grants), §8 (row filter, column mask, ABAC), §9 (lineage, auditoria, sharing) |
# | Microsoft Azure | §2 (Access Connector, storage credential, external location), §5 (Key Vault), §13 (soft delete do ADLS), seção ☁️ |
# | Arquitetura e desenvolvimento de pipelines | §4 (classificação como metadado), §9 (isolamento de ambientes), §13 (propagação do esquecimento) |
# | Python avançado | `src/oss_lakehouse/governance.py` — HMAC só com funções nativas do Spark, política como dado, testes |
# | IA aplicada à engenharia de dados | §7 (`limpar_texto_livre`, `mascara_formato_py`: o que o notebook 12 usa antes de mandar dado a um LLM) |
# | Times ágeis multidisciplinares | §14 (engenheiro de dados × DPO/encarregado × segurança × dono do dado) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Aviso de escopo:** as referências à LGPD (Lei 13.709/2018) estão aqui para o engenheiro conversar com o
# jurídico sabendo do que fala. Quem decide base legal, prazo e exceção é o controlador, com o encarregado (DPO).

# %% [markdown]
# ## Setup
#
# Tudo que é destrutivo acontece em `data/demo/11/`; a bronze compartilhada só é lida.
#
# **O titular deste notebook é fictício.** O GH Archive tem logins de pessoas reais. Para demonstrar o direito à
# eliminação sem apontar para ninguém, a tabela de demonstração troca o login de **uma** conta real por
# `zzzz-titular-ficticia` — em `actor_login`, no dono do `repo_name` e dentro do `payload` — e troca o nome dos
# repositórios dela por `repo-<código>`. A conta escolhida é a não-bot, com pelo menos 20 eventos próprios na
# hora das 12h, em cujos repositórios **outras** contas mais agiram (é o caso que interessa ao §13). Nas células
# em que logins reais apareceriam (o ataque do §5), a saída é mascarada. É a minimização (§10) aplicada ao
# próprio material.

# %%
import hashlib
import json
import shutil
import time
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from oss_lakehouse import governance as g
from oss_lakehouse.config import get_settings
from oss_lakehouse.spark import get_spark

s = get_settings()
DEMO = Path(s.data_root) / "demo" / "11"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True, exist_ok=True)

# warehouse próprio: as tabelas gerenciadas do §2 nascem (e morrem) dentro de data/demo/11
spark = get_spark("11", **{"spark.sql.warehouse.dir": str(DEMO / "warehouse")})

bronze = spark.read.format("delta").load(s.path("bronze", "gh_events"))
TITULAR = "zzzz-titular-ficticia"
TITULAR_ID = 999_000_111
EV = str(DEMO / "eventos")


def achatar(df):
    """Bronze (structs) → colunas planas, com os nomes usados em `POLITICA_GH_EVENTS`."""
    return df.select(
        "id",
        "type",
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("actor.url").alias("actor_url"),
        F.col("actor.avatar_url").alias("actor_avatar_url"),
        F.col("repo.name").alias("repo_name"),
        F.col("org.login").alias("org_login"),
        "payload",
        "created_at",
    )


h12 = achatar(bronze.where(F.col("_source_file").endswith("-12.json.gz")))

# Conta real que vira o titular fictício (o nome dela não é impresso): não-bot, >= 20 eventos próprios,
# e a que tem mais eventos de OUTRAS contas nos repositórios dela.
_proprios = h12.where(~F.col("actor_login").endswith("[bot]")).groupBy("actor_login").agg(F.count(F.lit(1)).alias("proprios"))
_de_outros = (
    h12.withColumn("dono", F.substring_index("repo_name", "/", 1))
    .where(F.col("dono") != F.col("actor_login"))
    .groupBy("dono")
    .agg(F.count(F.lit(1)).alias("de_outros"))
)
_orig = (
    _proprios.where("proprios >= 20")
    .join(_de_outros, F.col("actor_login") == F.col("dono"))
    .orderBy(F.desc("de_outros"), "actor_login")
    .first()["actor_login"]
)
_eh = F.col("actor_login") == _orig
(
    h12.withColumn("payload", F.replace("payload", F.lit(_orig), F.lit(TITULAR)))
    .withColumn(
        "repo_name",
        F.when(
            F.col("repo_name").startswith(_orig + "/"),
            F.concat(F.lit(TITULAR + "/repo-"), F.substring(F.sha2("repo_name", 256), 1, 6)),
        ).otherwise(F.col("repo_name")),
    )
    .withColumn("actor_id", F.when(_eh, F.lit(TITULAR_ID)).otherwise(F.col("actor_id")))
    .withColumn("actor_url", F.when(_eh, F.lit(f"https://api.github.com/users/{TITULAR}")).otherwise(F.col("actor_url")))
    .withColumn(
        "actor_avatar_url",
        F.when(_eh, F.lit(f"https://avatars.githubusercontent.com/u/{TITULAR_ID}?")).otherwise(F.col("actor_avatar_url")),
    )
    .withColumn("actor_login", F.when(_eh, F.lit(TITULAR)).otherwise(F.col("actor_login")))
    .repartition(4)
    .write.format("delta")
    .save(EV)
)
del _orig

ev = spark.read.format("delta").load(EV)
do_titular = F.col("actor_login") == TITULAR
n_total, n_logins = ev.count(), ev.select("actor_login").distinct().count()
n_ator = ev.where(do_titular).count()
n_repo = ev.where(~do_titular & F.col("repo_name").startswith(TITULAR + "/")).count()
n_payload = ev.where(~do_titular & F.col("payload").contains(TITULAR)).count()
IDS_TITULAR = [r.id for r in ev.where(do_titular).orderBy("id").limit(3).collect()]
print(f"Spark {spark.version} | eventos de demonstração: {n_total:,} linhas, {n_logins:,} logins distintos")
print(f"titular fictício '{TITULAR}':")
print(f"  {n_ator:>5} eventos em que ela é o ator (actor_login)")
print(f"  {n_repo:>5} eventos de OUTROS atores em repositórios dela (repo_name = '{TITULAR}/…')")
print(f"  {n_payload:>5} eventos de outros atores com o login dela dentro do payload (texto livre)")

# %% [markdown]
# Guarde esses três números: o titular aparece em **três lugares** da mesma tabela, e só um deles é a coluna
# "óbvia". Eles voltam no §8 (máscara que vaza) e no §13 (esquecimento incompleto).

# %% [markdown]
# ## 1. Unity Catalog: a hierarquia e o namespace de três níveis ☁️
#
# **O que é** — O **Unity Catalog (UC)** é a camada de governança do Databricks: um catálogo central de dados e
# de IA que guarda *quem pode o quê* (permissões), *o que é cada coisa* (metadados, tags), *de onde veio*
# (lineage — linhagem) e *quem acessou* (auditoria). Fica no nível da **conta**, acima dos workspaces.
#
# **Por que importa** — Antes do UC cada workspace tinha o seu Hive metastore: permissão por workspace, sem
# linhagem, sem auditoria central, e quem tinha a credencial do cluster lia o storage inteiro. Com o UC a
# permissão acompanha o dado, não o cluster — a mesma regra vale em qualquer workspace, SQL warehouse ou job.
#
# **Como funciona**
#
# > 📐 O visualizador de notebooks do GitHub não renderiza Mermaid — diagrama renderizado: [docs/diagramas.md](https://github.com/alanjoffre/oss-lakehouse/blob/main/docs/diagramas.md#nb11-1)
#
# ```mermaid
# flowchart TD
#     M["Metastore<br/>(1 por região, por conta)"] --> C1["Catalog<br/>prod"] & C2["Catalog<br/>dev"]
#     C1 --> S1["Schema<br/>bronze"] & S2["Schema<br/>silver"] & S3["Schema<br/>gold"]
#     S2 --> T["Table<br/>(managed ou external)"]
#     S2 --> V["View"]
#     S2 --> VO["Volume<br/>(arquivos)"]
#     S2 --> F["Function<br/>(UDF, máscara, filtro)"]
#     S2 --> MO["Model<br/>(ML registrado)"]
#     M -.-> SC["Storage credential"] -.-> EL["External location"]
# ```
#
# - **Metastore**: o contêiner de topo. Um por região; todos os workspaces da região se ligam a ele.
# - **Catalog** → **schema** (também chamado *database*) → objeto. Todo objeto tem nome de **três níveis**:
#   `catalogo.schema.objeto` — `prod.silver.gh_events`. `USE CATALOG prod; USE SCHEMA silver;` definem o padrão
#   da sessão, como `cd`.
# - Objetos de dados: **table**, **view**, **volume** (pasta governada para arquivos não tabulares — o lugar do
#   landing), **function** (UDF; é com função que se escreve máscara e filtro de linha) e **model**.
# - Objetos de acesso ao storage, direto no metastore: **storage credential** e **external location** (§2).
#
# O Spark open source deste repositório **não tem Unity Catalog**: o catálogo é o `spark_catalog` de sessão, com
# dois níveis (`schema.tabela`), sem permissões. A célula abaixo mostra o que ele responde.

# %%
print("catálogo atual:", spark.catalog.currentCatalog(), "| schema atual:", spark.catalog.currentDatabase())
print("catálogos:", [r.catalog for r in spark.sql("SHOW CATALOGS").collect()])
for sql in (
    "SELECT * FROM prod.silver.gh_events",
    "GRANT SELECT ON TABLE gh_events TO `analistas`",
    "SELECT is_account_group_member('pii-leitores')",
):
    try:
        spark.sql(sql).collect()
    except Exception as e:  # noqa: BLE001 - queremos mostrar o erro, qualquer que seja
        print(f"\n{sql}\n  → {type(e).__name__}: {' '.join(str(e).split())[:130]}")

# %% [markdown]
# Três erros, três ausências: não existe terceiro nível de nome, `GRANT` nem sequer é sintaxe válida, e a função
# que pergunta "o usuário é do grupo?" não existe. **Local, governança de acesso não se testa** — o que se testa
# é a transformação do dado (§5–§8) e o comportamento do Delta (§11–§13). O resto deste notebook marca ☁️ o que
# só existe na plataforma.
#
# > 🎤 **Resposta de 30 s:** "Unity Catalog é a governança central do Databricks, no nível da conta. A hierarquia
# > é metastore, catalog, schema e objeto — tabela, view, volume, função, modelo — e todo objeto tem nome de três
# > níveis. Permissão, tag, linhagem e auditoria ficam num lugar só e valem em todos os workspaces ligados ao
# > metastore. Um metastore por região; eu separo ambiente e domínio por catálogo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`hive_metastore`**: o catálogo legado aparece no UC como um catálogo à parte, sem as garantias do UC.
#   Migração: `SYNC`, `CREATE TABLE ... DEEP CLONE` ou o projeto UCX. *Hive Metastore Federation* expõe o legado
#   como catálogo estrangeiro enquanto se migra.
# - **Lakehouse Federation**: catálogos estrangeiros (`CREATE FOREIGN CATALOG`) apontam para PostgreSQL, SQL
#   Server, Snowflake etc. — consulta com governança do UC, sem copiar o dado.
# - **Access mode do compute**: *standard* (antigo *shared*) isola usuários e aplica tudo; *dedicated* (antigo
#   *single user*) depende de filtragem no serverless para row filter e column mask (§8). Compute sem modo de
#   acesso do UC não enxerga o UC.
# - **Unity Catalog open source**: existe um projeto na Linux Foundation com o mesmo nome e API compatível; não
#   é usado aqui porque não traz o motor de permissões da plataforma — simular daria falsa segurança.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Um metastore por região: dado em duas regiões = dois metastores, e o compartilhamento entre eles é por
#   Delta Sharing (§9), não por `GRANT`.
# - Catálogo demais vira burocracia; de menos, vira `GRANT` tabela a tabela. O corte comum é ambiente × domínio.

# %% [markdown]
# ## 2. Managed × external, storage credential e external location 🧪 + ☁️
#
# **O que é** — Tabela **managed** (*gerenciada*): o catálogo é dono dos arquivos e escolhe onde eles ficam.
# Tabela **external** (*externa*): você diz o caminho (`LOCATION`); o catálogo guarda só o metadado.
#
# **Por que importa** — A diferença aparece no `DROP TABLE`: na gerenciada os arquivos vão embora, na externa
# ficam. Para LGPD isso decide se "apaguei a tabela" significa "apaguei o dado".
#
# **Como funciona** — O comportamento do `DROP` é do Spark, então dá para provar local: uma tabela de cada tipo,
# um `DROP` em cada, e olhar o disco.

# %%
ext_dir = DEMO / "externa"
spark.sql("CREATE SCHEMA IF NOT EXISTS demo11")
spark.sql("CREATE TABLE demo11.gerenciada (id INT, login STRING) USING delta")
spark.sql(f"CREATE TABLE demo11.externa (id INT, login STRING) USING delta LOCATION '{ext_dir}'")
for t in ("gerenciada", "externa"):
    spark.sql(f"INSERT INTO demo11.{t} VALUES (1, '{TITULAR}')")
    d = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE EXTENDED demo11.{t}").collect()}
    print(f"{t:<11} tipo={d['Type']:<9} local=…/demo/11/{d['Location'].split('/demo/11/')[-1]}")
ger_dir = DEMO / "warehouse" / "demo11.db" / "gerenciada"

spark.sql("DROP TABLE demo11.gerenciada")
spark.sql("DROP TABLE demo11.externa")
print("\ndepois do DROP TABLE:")
print(f"  gerenciada: pasta existe? {ger_dir.exists()}")
print(f"  externa   : pasta existe? {ext_dir.exists()}  | parquet restantes: {len(list(ext_dir.glob('*.parquet')))}")
print(f"  titular ainda legível na externa: {spark.read.format('delta').load(str(ext_dir)).where(F.col('login') == TITULAR).count()} linha")

# %% [markdown]
# O `DROP` da externa removeu o registro do catálogo e deixou o parquet — com o titular dentro — legível por
# qualquer um com acesso à pasta.
#
# **No Unity Catalog ☁️** a regra é a mesma, com três diferenças que caem em entrevista:
#
# | | Managed (UC) | External (UC) |
# |---|---|---|
# | Onde ficam os arquivos | no *managed storage* do schema → catálogo → metastore (o mais específico vence) | no caminho que você indicou, dentro de uma *external location* |
# | `DROP TABLE` | arquivos apagados depois de 7 dias (janela do `UNDROP TABLE`) | arquivos ficam |
# | Formato | Delta (ou Iceberg gerenciado) | Delta, Parquet, CSV, JSON… |
# | Manutenção automática | *predictive optimization* roda `OPTIMIZE`/`VACUUM` sozinha | por sua conta |
# | Quando usar | padrão | o storage é lido/escrito por outro sistema, ou o caminho é exigência |
#
# Como o UC chega ao ADLS sem chave no código:
#
# ```sql
# -- ☁️ 1) Storage credential: embrulha a identidade gerenciada do Access Connector for Azure Databricks,
# --       que tem o papel "Storage Blob Data Contributor" na conta de storage (criada por Terraform — nb 14).
# CREATE STORAGE CREDENTIAL cred_lake
#   WITH (AZURE_MANAGED_IDENTITY (ACCESS_CONNECTOR_ID =
#     '/subscriptions/<sub>/resourceGroups/rg-lake/providers/Microsoft.Databricks/accessConnectors/ac-lake'));
#
# -- ☁️ 2) External location: um caminho + a credencial que o acessa. É o objeto que recebe GRANT.
# CREATE EXTERNAL LOCATION loc_landing
#   URL 'abfss://landing@stlakeprod.dfs.core.windows.net/'
#   WITH (STORAGE CREDENTIAL cred_lake);
# GRANT READ FILES ON EXTERNAL LOCATION loc_landing TO `sp-ingestao`;
#
# -- ☁️ 3) Catálogo com storage gerenciado próprio (isola os dados de prod dos de dev no nível do contêiner)
# CREATE CATALOG prod MANAGED LOCATION 'abfss://prod@stlakeprod.dfs.core.windows.net/managed';
#
# -- ☁️ 4) Tabela externa e volume externo sobre a location
# CREATE TABLE prod.bronze.gh_events_ext LOCATION 'abfss://landing@stlakeprod.dfs.core.windows.net/gh_events';
# CREATE EXTERNAL VOLUME prod.landing.gharchive LOCATION 'abfss://landing@stlakeprod.dfs.core.windows.net/gharchive';
# ```
#
# > 🎤 **Resposta de 30 s:** "Managed: o Unity Catalog é dono dos arquivos, o DROP apaga o dado — com janela de
# > UNDROP de 7 dias — e a plataforma faz a manutenção. External: eu informo o caminho e o DROP só tira o
# > registro. O padrão é managed; external só quando outro sistema precisa do mesmo caminho. O acesso ao ADLS é
# > por storage credential, que embrulha a identidade gerenciada do Access Connector, e external location, que
# > é o caminho mais a credencial. Ninguém recebe chave de storage."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Sobreposição de caminhos**: o UC não deixa duas external locations (ou uma location e um managed storage)
#   se sobreporem — cada caminho tem um único dono de governança.
# - **Acesso por caminho**: com UC, ler `abfss://…` direto exige `READ FILES` na external location; e tabela com
#   row filter ou column mask **não** aceita leitura por caminho (§8). Por isso o código deste repo usa caminho
#   só local; no Databricks o `Settings` troca para nome de três níveis.
# - **Credencial temporária**: o UC entrega ao compute um token de curta duração com escopo no caminho da
#   tabela (*credential vending*). O cluster não tem credencial permanente do storage.
# - **`UNDROP` e LGPD**: tabela gerenciada dropada continua recuperável por 7 dias. "Dropei" não é "eliminei".
# </details>
#
# **Trade-offs / quando NÃO usar**
# - External dá liberdade de caminho e cobra em governança: arquivo órfão depois do `DROP`, `VACUUM` manual.
# - Managed prende o layout físico ao UC: ferramenta que só lê caminho precisa de Delta Sharing ou de credencial
#   temporária, não do `abfss://` direto.

# %% [markdown]
# ## 3. Grants, herança, grupos, ownership e menor privilégio ☁️
#
# **O que é** — O UC é um modelo de **privilégios aditivos** sobre objetos *securable* (que aceitam permissão):
# `GRANT <privilégio> ON <objeto> TO <principal>`. *Principal* é um usuário, um **service principal** (identidade
# de aplicação — é quem roda job em produção) ou um **grupo**.
#
# **Por que importa** — *Menor privilégio* (least privilege): cada identidade tem só o que precisa para a sua
# função. É o que limita o estrago de uma credencial vazada e é o que a auditoria pergunta primeiro.
#
# **Como funciona**
#
# - **Herança para baixo**: privilégio dado no catálogo ou no schema vale para todos os objetos filhos,
#   **atuais e futuros**. `GRANT SELECT ON SCHEMA prod.gold` cobre a tabela criada amanhã.
# - **`USE CATALOG` e `USE SCHEMA` são a porta**: sem eles nos pais, `SELECT` na tabela não serve para nada.
#   Eles não dão acesso a dado nenhum — só permitem "atravessar".
# - **`BROWSE`**: ver que o objeto existe e seus metadados, sem ler o dado — é o que permite descoberta no
#   catálogo e pedido de acesso.
# - **Ownership**: todo objeto tem um dono, que tem todos os privilégios sobre ele e pode concedê-los. Dono deve
#   ser **grupo**, nunca pessoa (a pessoa sai da empresa). `MANAGE` delega a administração de permissões sem
#   passar a propriedade.
# - **Grupos**: só grupos **de conta** recebem `GRANT` no UC (grupo local de workspace é legado). Eles vêm do
#   Microsoft Entra ID por **SCIM** (protocolo padrão de provisionamento de identidades) ou pela sincronização
#   automática de identidades do Azure Databricks. A entrada e a saída de pessoas acontecem no Entra ID; o
#   `GRANT` nunca muda.
#
# ```sql
# -- ☁️ Analistas: leem a gold de prod, e mais nada.
# GRANT USE CATALOG ON CATALOG prod             TO `analistas`;
# GRANT USE SCHEMA  ON SCHEMA  prod.gold        TO `analistas`;
# GRANT SELECT      ON SCHEMA  prod.gold        TO `analistas`;   -- herda para as tabelas atuais e futuras
#
# -- ☁️ Service principal do pipeline: lê a bronze, escreve na silver. Não é dono de nada, não lê a gold.
# GRANT USE CATALOG ON CATALOG prod             TO `sp-pipeline-silver`;
# GRANT USE SCHEMA, SELECT ON SCHEMA prod.bronze TO `sp-pipeline-silver`;
# GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.silver TO `sp-pipeline-silver`;
#
# -- ☁️ Descoberta sem leitura, para todo mundo; propriedade e administração com grupos.
# GRANT BROWSE ON CATALOG prod TO `account users`;
# ALTER SCHEMA prod.silver OWNER TO `eng-dados-admins`;
# GRANT MANAGE ON SCHEMA prod.silver TO `governanca-dados`;
#
# -- ☁️ Conferir e retirar
# SHOW GRANTS ON TABLE prod.gold.fct_eventos_diarios;
# SHOW GRANTS `analistas` ON SCHEMA prod.gold;
# REVOKE SELECT ON SCHEMA prod.gold FROM `analistas`;
# ```
#
# | Privilégio | Em quê | O que permite |
# |---|---|---|
# | `USE CATALOG` / `USE SCHEMA` | catálogo / schema | atravessar (pré-requisito de todos os outros) |
# | `SELECT` | tabela, view | ler |
# | `MODIFY` | tabela | `INSERT`, `UPDATE`, `DELETE`, `MERGE` |
# | `CREATE TABLE`, `CREATE SCHEMA`… | schema, catálogo | criar filhos |
# | `EXECUTE` | função, modelo | chamar |
# | `READ VOLUME` / `WRITE VOLUME` | volume | ler / gravar arquivos |
# | `READ FILES` / `WRITE FILES` | external location | acesso por caminho |
# | `BROWSE` | catálogo e abaixo | ver metadados sem ler dado |
# | `APPLY TAG` | objeto | aplicar tags |
# | `MANAGE` | objeto | administrar permissões, sem ser o dono |
# | `ALL PRIVILEGES` | qualquer | tudo que existir naquele nível — evite |
#
# A evidência local deste tópico é o erro do §1: `GRANT` não existe no Spark open source. Não há como "simular"
# permissão no laptop sem mentir; o que este repositório faz é **versionar** os grants (Terraform, notebook 14)
# para que eles passem por pull request como qualquer código.
#
# > 🎤 **Resposta de 30 s:** "Privilégio é aditivo e herda para baixo: dou SELECT no schema e ele vale para as
# > tabelas atuais e futuras. USE CATALOG e USE SCHEMA são pré-requisito, não dão dado. Eu concedo sempre a
# > grupo de conta, que vem do Entra ID por SCIM; job roda como service principal com o mínimo — lê a camada
# > anterior, escreve na sua; dono de objeto é grupo. E os grants ficam em Terraform, revisados em PR."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Não existe `DENY` no `GRANT`**: como é aditivo, tira-se acesso revogando ou não concedendo. Consequência:
#   `GRANT SELECT ON CATALOG` é difícil de "furar" para uma tabela sensível — a tabela sensível vai para outro
#   schema, ou recebe row filter/column mask. (A documentação de 29/09/2026 lista *DENY policies* do ABAC em
#   Beta, restritas ao privilégio `MANAGE ACCESS CONTROL`.)
# - **View como fronteira**: quem consulta uma view precisa de `SELECT` na view, não nas tabelas de baixo —
#   desde que o **dono da view** tenha acesso a elas. É o mecanismo da *dynamic view* (§8).
# - **Metastore admin e account admin** são papéis de emergência, não de rotina. Em produção, o dia a dia é de
#   grupos donos de catálogo/schema.
# - **`information_schema`**: `prod.information_schema.table_privileges` e afins permitem auditar grants por
#   SQL — a base de um teste automático de "ninguém fora do grupo X tem SELECT na bronze".
# - **Workspace-catalog binding**: restringe em quais workspaces um catálogo é acessível (§9).
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Grant no catálogo é confortável e largo demais; grant por tabela é preciso e vira manutenção infinita.
#   O ponto de equilíbrio é o **schema**, com as tabelas agrupadas por sensibilidade.
# - Herança cobre tabelas futuras: bom para a gold, perigoso num schema que um dia recebe dado sensível.

# %% [markdown]
# ## 4. Classificação de dados como metadado 🧪 (e tags governadas ☁️)
#
# **O que é** — **Classificar** é dizer, coluna a coluna, quão sensível o dado é. A escala corporativa usual tem
# quatro níveis:
#
# | Nível | Critério | Exemplo na bronze |
# |---|---|---|
# | **público** | pode sair da empresa sem dano | `type`, `created_at`, `org_login` |
# | **interno** | uso interno, dano baixo se vazar | `repo_name` |
# | **confidencial** | dado pessoal comum, segredo de negócio | `actor_login`, `actor_id`, `payload` |
# | **restrito** | dado pessoal sensível, documento, credencial | (não há na bronze) |
#
# **Por que importa** — Máscara, filtro, retenção e esquecimento dependem de saber **onde** está o dado pessoal.
# Sem classificação gravada junto da tabela, essa resposta mora na cabeça de alguém.
#
# **Como funciona** — A política é **dado**, não código espalhado: `POLITICA_GH_EVENTS` é uma tupla de
# `PoliticaColuna(coluna, classificacao, dado_pessoal, tratamento, motivo)`. A mesma tupla alimenta os
# comentários da tabela (aqui), a view por papel (§8) e o SQL de tags do UC (abaixo).
#
# Sem UC, o Delta open source oferece dois lugares para o metadado, ambos gravados no `_delta_log`:
# **comentário de coluna** e **propriedades de tabela** (`TBLPROPERTIES`).

# %%
marcadas = g.aplicar_classificacao_delta(spark, EV, g.POLITICA_GH_EVENTS)
print(f"{len(marcadas)} colunas classificadas\n")
spark.sql(f"DESCRIBE TABLE delta.`{EV}`").show(truncate=False)

# %%
props = g.ler_classificacao_delta(spark, EV)
for k in sorted(props):
    print(f"{k:<45} {props[k]}")

show = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES delta.`{EV}`").collect()}
print("\no mesmo, lido por SHOW TBLPROPERTIES → actor_url:", show["governanca.classificacao.actor_url"])

hist = spark.sql(f"DESCRIBE HISTORY delta.`{EV}`")
print()
hist.groupBy("operation").count().orderBy("operation").show()
print("arquivos de dados na versão atual:", len(ev.inputFiles()), "(os mesmos 4 da escrita: classificar não reescreve dado)")

# %% [markdown]
# Cada `ALTER` é um commit só de metadado (10 `CHANGE COLUMN` e 1 `SET TBLPROPERTIES`): os arquivos de dados
# não mudam. Um job de auditoria varre as propriedades de todas as tabelas e responde "quais tabelas têm
# dado pessoal?" sem abrir nenhuma.
#
# Detalhe que só aparece rodando: `ler_classificacao_delta` lê por `DESCRIBE DETAIL`, porque o
# `SHOW TBLPROPERTIES` devolve `*********(redacted)` para `actor_url` (linha do meio da saída). O Spark redige
# toda propriedade cuja chave ou valor pareça segredo — a expressão padrão inclui `secret`, `password`, `token`
# e `url` — e o nome da coluna caiu nela. Um job de auditoria que lesse pelo `SHOW` perderia justamente as
# colunas de URL.
#
# **O limite, dito com clareza:** comentário e propriedade são **documentação**. Nada aqui impede a leitura.
# No Unity Catalog a mesma classificação vira **tag**, e tag pode virar **política** (ABAC, §8). O módulo gera o
# SQL a partir da mesma tupla:

# %%
for linha in g.sql_tags_uc("prod.bronze.gh_events", g.POLITICA_GH_EVENTS)[1:5]:
    print(linha)

# %% [markdown]
# ```sql
# -- ☁️ Tag em tabela e em schema (tags de catálogo/schema são herdadas pelos filhos)
# ALTER TABLE  prod.bronze.gh_events SET TAGS ('contem_dado_pessoal' = 'true', 'retencao_dias' = '90');
# ALTER SCHEMA prod.bronze           SET TAGS ('camada' = 'bronze');
#
# -- ☁️ Onde está o dado pessoal da empresa inteira? Uma consulta.
# SELECT catalog_name, schema_name, table_name, column_name, tag_value
# FROM system.information_schema.column_tags
# WHERE tag_name = 'pii' AND tag_value = 'true';
# ```
#
# **Tag comum × tag governada ☁️** — Tag comum é texto livre: qualquer um com `APPLY TAG` escreve `PII`, `pii`
# ou `Pii=sim`. **Tag governada** (*governed tag*) é definida no nível da **conta**, com lista de valores
# permitidos e controle de quem pode atribuir. Só tag governada pode ser usada em política ABAC — faz sentido:
# se a tag decide quem vê o dado, quem pode mudar a tag tem de ser controlado.
#
# > 🎤 **Resposta de 30 s:** "Classifico por coluna em quatro níveis — público, interno, confidencial, restrito —
# > e marco o que é dado pessoal. A política fica em código, versionada, e gera o metadado: local, comentário de
# > coluna e propriedade de tabela Delta; no Unity Catalog, tags. Tag governada tem valores controlados na conta
# > e é o que alimenta política ABAC: taguei a coluna como PII, a máscara vale sozinha. Sem classificação não dá
# > nem para responder onde está o dado de um titular."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Classificação automática**: o Databricks tem *data classification* que sugere tags de PII por amostragem;
#   é ponto de partida, não substitui revisão. O notebook 12 mostra a versão com LLM — que recebe a amostra já
#   com `mascara_formato_py`, nunca o valor.
# - **Sensibilidade por combinação**: `created_at` é público, `repo_name` é interno; juntos identificam uma
#   pessoa (§6, k-anonimato). Classificar coluna a coluna é necessário e não suficiente.
# - **Texto livre**: `payload` é confidencial porque *pode* conter qualquer coisa (e-mail em mensagem de commit,
#   nome em issue). A política o trata como o pior caso do que cabe ali.
# - **Onde a política mora**: num repositório com dono (governança) e *pull request* — a mudança de
#   "confidencial" para "interno" precisa de revisão, como qualquer mudança de permissão.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Comentário de coluna é visível e frágil (alguém sobrescreve com a descrição de negócio). Propriedade de
#   tabela é mais estável; por isso `aplicar_classificacao_delta` grava nos dois.
# - Classificar tudo como "restrito" por precaução esvazia a escala: ninguém consegue trabalhar e as exceções
#   viram a regra.

# %% [markdown]
# ## 5. Pseudonimização: hash sem sal × HMAC com chave 🧪
#
# **O que é** — **Pseudonimizar** é trocar o identificador por um substituto, de forma que a ligação com a
# pessoa dependa de uma **informação adicional guardada em separado** (LGPD art. 13 §4º). O substituto precisa
# ser **determinístico** (o mesmo login vira sempre o mesmo código) para servir de chave de junção.
#
# Duas formas de fazer, que parecem iguais e não são:
#
# - **Hash sem sal**: `sha256(login)`. Determinístico, sem segredo nenhum.
# - **HMAC** (*hash-based message authentication code* — hash com chave, RFC 2104): `HMAC-SHA256(chave, login)`.
#   Determinístico **para quem tem a chave**.
#
# **Por que importa** — "A gente anonimiza com hash" é uma das frases mais ditas e mais erradas da área. Hash não
# é criptografia e não esconde nada quando o conjunto de valores possíveis é pequeno ou **público** — e a lista
# de logins do GitHub é pública.
#
# **Como funciona o ataque de dicionário** — O atacante não "inverte" o SHA-256. Ele calcula o hash de cada
# candidato e faz um `JOIN`:
#
# ```text
# dataset "anonimizado"            dicionário do atacante (público)
#   login_hash | type                login      → sha256(login)
#   3fa9…      | PushEvent    ⨝      octocat    → 3fa9…          ⇒  3fa9… = octocat
# ```
#
# **A demonstração**: publicamos os eventos das 12h com `sha256(login)` no lugar do login. O atacante tem os
# logins que aparecem nas **outras duas horas** da bronze (13h e 14h) — uma fonte pública qualquer serve.

# %%
try:
    g.obter_chave()
    print("OSSLH_PSEUDO_KEY definida no ambiente")
except RuntimeError as e:
    print("sem segredo no ambiente →", e)
chave = g.obter_chave(permitir_demo=True)  # só o notebook pede a chave de demonstração, e de forma explícita

real_12 = bronze.where(F.col("_source_file").endswith("-12.json.gz")).select(F.col("actor.login").alias("login"), "type")
publicado = real_12.select(
    g.sha256_sem_sal("login").alias("login_hash"), g.hmac_sha256("login", chave).alias("login_hmac"), "type"
).cache()
dicionario = (
    bronze.where(~F.col("_source_file").endswith("-12.json.gz")).select(F.col("actor.login").alias("login")).distinct().cache()
)
publicado.select("login_hash", "type").show(3, truncate=28)

# %%
t0 = time.perf_counter()
recuperados = g.ataque_dicionario(publicado, "login_hash", dicionario, "login").cache()
n_rec = recuperados.count()
dt_ataque = time.perf_counter() - t0
n_hash = publicado.select("login_hash").distinct().count()
ev_total = publicado.count()
ev_rec = publicado.join(recuperados, "login_hash", "left_semi").count()
print(f"dicionário do atacante: {dicionario.count():,} logins vistos nas horas 13 e 14")
print(f"ataque: {dt_ataque:.1f}s")
print(f"logins reidentificados: {n_rec:,} de {n_hash:,} ({n_rec / n_hash:.1%})")
print(f"eventos reidentificados: {ev_rec:,} de {ev_total:,} ({ev_rec / ev_total:.1%})")
print("\namostra (login mascarado na saída; o atacante vê o valor inteiro):")
recuperados.orderBy("login_hash").limit(5).select(
    F.substring("login_hash", 1, 20).alias("login_hash (início)"),
    g.mascara_parcial("valor_recuperado", 3).alias("login recuperado"),
).show(truncate=False)

# %% [markdown]
# Com um dicionário montado em **duas horas** de dado público, o atacante reidentificou **30,8% dos logins** e
# **60,2% dos eventos** — em segundos, num laptop, com um `JOIN`. A fração de eventos é o dobro da de logins
# porque quem aparece em várias horas é justamente quem gera mais eventos.
#
# E o dicionário completo? O `actor.id` do GitHub é sequencial: o maior id da bronze dá o tamanho do universo.
# A célula mede quantos SHA-256 um núcleo deste laptop calcula por segundo e faz a conta.

# %%
max_id = bronze.agg(F.max("actor.id")).first()[0]
n = 300_000
t0 = time.perf_counter()
for i in range(n):
    hashlib.sha256(b"usuario-%d" % i).hexdigest()
taxa = n / (time.perf_counter() - t0)
print(f"maior actor.id na bronze: {max_id:,}  → ordem de grandeza do nº de contas do GitHub")
print(f"SHA-256 em 1 núcleo (Python puro): {taxa:,.0f} hashes/s")
print(f"hash de TODAS as contas: ~{max_id / taxa / 60:.0f} min em 1 núcleo")

# %% [markdown]
# Ou seja: são cerca de 336 milhões de ids, e um núcleo em Python puro faz o hash de todos em minutos. Quem
# tiver a lista de logins (o GitHub a entrega pela API pública) reverte **100%** do dataset. Hash sem sal de identificador público é ofuscação, não proteção.
#
# **Agora o HMAC.** Mesmo ataque, três atacantes: sem a chave, com um palpite de chave, e com a chave certa.

# %%
def reidentifica_hmac(chave_do_atacante: bytes) -> int:
    tabela = dicionario.select(g.hmac_sha256("login", chave_do_atacante).alias("login_hmac"))
    return publicado.select("login_hmac").distinct().join(tabela, "login_hmac").count()


print(f"sem a chave (sha256 puro contra a coluna HMAC): {g.ataque_dicionario(publicado, 'login_hmac', dicionario, 'login').count():,}")
print(f"com palpite de chave (b'github')              : {reidentifica_hmac(b'github'):,}")
print(f"COM a chave certa                             : {reidentifica_hmac(chave):,}  ← igual ao ataque do hash")

d = real_12.agg(F.countDistinct("login").alias("logins"), F.countDistinct(g.hmac_sha256("login", chave)).alias("pseudonimos")).first()
print(f"\nlogins distintos: {d.logins:,} | pseudônimos distintos: {d.pseudonimos:,}  (1 para 1: serve de chave de junção)")
print("HMAC nativo do Spark == hmac do Python:", publicado.where(F.col("login_hmac") == g.hmac_sha256_py("github-actions[bot]", chave)).count() > 0)

# %% [markdown]
# Três leituras:
#
# 1. **Sem a chave, zero.** O atacante precisa adivinhar a chave — 256 bits aleatórios, não uma palavra.
# 2. **Com a chave, tudo volta** — os mesmos 11.801 logins do ataque ao hash. Por isso dado pseudonimizado
#    **continua sendo dado pessoal** para quem controla a chave: a proteção é a chave estar em outro lugar.
# 3. O pseudônimo é 1 para 1 com o login: `JOIN` e `COUNT(DISTINCT)` continuam funcionando na silver e na gold.
#
# **Onde a chave mora** — `obter_chave()` lê a variável `OSSLH_PSEUDO_KEY` e **falha** se ela não existir (é a
# primeira linha impressa nesta seção): pseudonimizar com uma chave conhecida é pior que não rodar.
# No Azure ☁️ a chave fica no **Key Vault**, exposta ao Databricks por um *secret scope*:
#
# ```python
# # ☁️ o valor nunca aparece em log nem em saída de notebook (o Databricks o redige como [REDACTED])
# chave = dbutils.secrets.get(scope="kv-lakehouse", key="pseudo-hmac-key").encode()
# silver = bronze.withColumn("actor_pseudo_id", hmac_sha256("actor.login", chave))
# ```
#
# `hmac_sha256` é escrita só com funções nativas (`sha2`, `concat`, `unhex`): o cálculo roda na JVM. A
# alternativa óbvia — uma UDF Python com `hmac.new` — serializa cada linha entre JVM e Python.
#
# > 🎤 **Resposta de 30 s:** "Hash sem sal não protege identificador: o atacante faz o hash da lista de
# > candidatos e dá um join — é o ataque de dicionário, e para login, CPF, e-mail e telefone a lista existe ou
# > é enumerável. Eu uso HMAC-SHA256 com chave guardada no Key Vault: continua determinístico, então serve de
# > chave de junção, e sem a chave não reverte. E deixo claro que isso é pseudonimização, não anonimização: quem
# > tem a chave reidentifica, então o dado segue sendo pessoal na LGPD."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **"E se eu puser um sal?"** Sal **global e conhecido** (no código, no repositório) não muda nada: o atacante
#   concatena o sal. Sal **secreto** é uma chave mal usada — `sha256(sal || valor)` é vulnerável a ataque de
#   extensão de comprimento; o HMAC existe para isso. Sal **aleatório por linha** (como em senha) impede o
#   dicionário pré-computado, mas destrói a junção: o mesmo login vira códigos diferentes.
# - **CPF é pior que login**: são 10⁹ combinações válidas de 9 dígitos (os 2 últimos são verificadores). Hash
#   sem sal de CPF se quebra por força bruta completa, sem lista nenhuma.
# - **Rotação de chave**: trocar a chave muda todos os pseudônimos — as tabelas derivadas precisam ser
#   reprocessadas ou conviver com `versao_chave`. Planeje antes: guarde a versão da chave junto do pseudônimo.
# - **Chave por ambiente**: dev e prod com chaves diferentes impedem que um dump de dev seja cruzado com prod.
# - **Chave por finalidade**: pseudônimos diferentes para marketing e para antifraude impedem a junção entre
#   bases que não deveriam se cruzar.
# - **Truncar o hash** para "economizar" aumenta colisão: dois titulares com o mesmo pseudônimo é incidente de
#   qualidade *e* de privacidade.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - HMAC não reverte: se o negócio precisa **recuperar** o valor (suporte, cobrança), é tokenização ou
#   criptografia (§6).
# - Determinismo é o que permite junção e também o que permite **análise de frequência**: o pseudônimo com 11%
#   dos eventos é o `github-actions[bot]`, com ou sem chave.

# %% [markdown]
# ## 6. Tokenização, criptografia, generalização e k-anonimato 🧪
#
# **O que é** — As outras técnicas da caixa de ferramentas, cada uma com um perfil de reversão:
#
# | Técnica | Como funciona | Reverte? | Serve de chave de junção? | Uso típico |
# |---|---|---|---|---|
# | Hash sem sal | `sha256(valor)` | sim, por dicionário (§5) | sim | nunca para dado pessoal |
# | HMAC com chave | `HMAC(chave, valor)` | não, sem a chave | sim | pseudônimo analítico |
# | **Tokenização** | valor ↔ token **aleatório**, guardado num **cofre** (*token vault*) | sim, por quem lê o cofre | sim | cartão, CPF em sistemas que precisam do valor de volta |
# | **Criptografia** | `AES-256-GCM(chave, valor)` | sim, por quem tem a chave | não (cada cifra é diferente) | guardar o valor; crypto-shredding (§13) |
# | **Generalização** | trocar o valor por uma faixa (idade → faixa, segundo → hora) | não | não | publicação, estatística |
#
# **Por que importa** — A pergunta de entrevista é "qual você usaria?" e a resposta certa começa com "depende de
# quem precisa reverter".
#
# **Como funciona: tokenização.** O token não é *calculado* a partir do valor — é sorteado e a correspondência
# fica numa tabela. Sem o cofre, o token não carrega informação nenhuma; não há o que atacar com dicionário.

# %%
COFRE = str(DEMO / "cofre_tokens")
g.construir_cofre_tokens(ev, "actor_login").write.format("delta").save(COFRE)  # gravar ANTES de usar
cofre = spark.read.format("delta").load(COFRE)

tokenizado = g.tokenizar(ev.select("id", "type", "actor_login"), "actor_login", cofre)
tokenizado.where(F.col("id").isin(IDS_TITULAR)).orderBy("id").show(truncate=False)

token_do_titular = tokenizado.where(F.col("id") == IDS_TITULAR[0]).first()["actor_login"]
print("reversão por quem lê o cofre:")
cofre.where(F.col("token") == token_do_titular).show(truncate=False)

outro_cofre = g.construir_cofre_tokens(ev, "actor_login")
print(f"tokens iguais entre o cofre gravado e um recalculado: {cofre.join(outro_cofre, ['valor', 'token']).count()} de {cofre.count():,}")

# %% [markdown]
# A última linha é o detalhe que derruba implementações: `uuid()` é **não determinístico**. Se o cofre for um
# DataFrame recalculado (por *retry* de task ou por reuso sem persistir), cada execução sorteia tokens novos e as
# junções deixam de bater. O cofre precisa ser **gravado** e lido de volta — e em produção, alimentado por
# `MERGE` (só insere token para valor novo).
#
# **Criptografia.** `aes_encrypt` usa AES-GCM (modo autenticado: detecta adulteração) com vetor de inicialização
# aleatório. Consequência: cifrar o mesmo valor duas vezes dá resultados diferentes.

# %%
k_hex = hashlib.sha256(chave).hexdigest()  # 32 bytes = AES-256
claro = spark.createDataFrame([(TITULAR, k_hex, hashlib.sha256(b"outra").hexdigest())], "login string, k string, k_errada string")
c1 = claro.select(g.criptografar("login", "k")).first()[0]  # duas execuções independentes
c2 = claro.select(g.criptografar("login", "k")).first()[0]
print(f"cifra 1: {c1}\ncifra 2: {c2}\niguais? {c1 == c2}")
claro.withColumn("c", F.lit(c1)).select(
    g.descriptografar("c", "k").alias("abre com a chave"), g.descriptografar("c", "k_errada").alias("abre com outra chave")
).show(truncate=False)

# %% [markdown]
# Cifras diferentes para o mesmo valor, em duas execuções: criptografia **não serve de chave de junção** (e isso é uma qualidade —
# não permite análise de frequência). O padrão é guardar os dois: o pseudônimo HMAC para juntar e contar, e o
# valor cifrado para o caso raro de precisar abrir. `try_aes_decrypt` devolve `NULL` com a chave errada em vez
# de derrubar o job — é o que o crypto-shredding do §13 explora.
#
# **Generalização e k-anonimato.** Tirar o identificador não anonimiza se as colunas restantes, **combinadas**,
# apontam para uma pessoa. Essas colunas são os **quase-identificadores** (*quasi-identifiers*). Uma tabela tem
# **k-anonimato** quando cada combinação de quase-identificadores aparece em pelo menos **k** linhas — cada
# pessoa se esconde entre k−1 outras. A célula mede o k da tabela de eventos **sem nenhuma coluna de ator**, em
# três níveis de generalização.

# %%
base = ev.select(F.to_timestamp("created_at").alias("ts"), "repo_name", "type")
niveis = {
    "segundo + repositório + tipo": base,
    "minuto + dono do repositório + tipo": base.select(
        F.date_trunc("minute", "ts").alias("minuto"), F.substring_index("repo_name", "/", 1).alias("dono"), "type"
    ),
    "hora + tipo": base.select(F.date_trunc("hour", "ts").alias("hora"), "type"),
}
_n = F.count(F.lit(1)).over(Window.partitionBy("hora", "type"))
niveis["hora + tipo, suprimindo grupos < 10"] = niveis["hora + tipo"].withColumn("_n", _n).where("_n >= 10").drop("_n")
linhas = [f"{'quase-identificadores':<38}{'k':>6}{'grupos':>9}{'grupos de 1 linha':>19}{'% linhas únicas':>17}{'linhas':>9}"]
for nome, d in niveis.items():
    m = g.medir_k_anonimato(d, d.columns)
    linhas.append(f"{nome:<38}{m['k']:>6,}{m['grupos']:>9,}{m['grupos_unicos']:>19,}{m['grupos_unicos'] / m['linhas']:>16.1%}{m['linhas']:>9,}")
print("\n".join(linhas))

# %% [markdown]
# Sem login, sem id, sem URL — e ainda assim, no nível mais fino, **99,2%** das linhas são únicas: quem sabe
# que "alguém fez push no repositório X às 12:03:24" encontra a linha e, com ela, tudo o que estiver ao lado.
# Generalizar para minuto e dono do repositório ainda deixa 78,7% das linhas únicas. E o resultado que contraria
# a intuição: mesmo no nível mais grosso — hora + tipo, 16 grupos — o **k continua 1**, porque um tipo de
# evento raro aconteceu uma única vez naquela hora. Generalizar não basta; é preciso **suprimir** os grupos
# pequenos (última linha: descartando os 3 tipos com menos de 10 eventos — 11 linhas — o k sobe para 24).
# Nesse ponto a tabela já não
# responde quase nenhuma pergunta de negócio. É o **trade-off de utilidade**: não existe anonimização útil de
# graça, e o k se **mede** — não se supõe.
#
# > 🎤 **Resposta de 30 s:** "Depende de quem precisa reverter. Ninguém: HMAC com chave, que ainda serve para
# > junção. Um time específico, com auditoria: tokenização, com o cofre separado e de acesso restrito. Preciso
# > guardar o valor: criptografia AES-GCM, com a chave no Key Vault — sabendo que a cifra não serve para join.
# > Para publicar dado: generalização e supressão até atingir um k mínimo, medindo o k, porque tirar o nome não
# > anonimiza — os quase-identificadores reidentificam."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Limites do k-anonimato**: se todas as k pessoas do grupo têm o mesmo atributo sensível, o grupo o revela
#   (*homogeneity attack*) — daí *l-diversity* e *t-closeness*. E o k é medido contra a própria tabela: uma base
#   externa pode quebrar o grupo.
# - **Privacidade diferencial** (*differential privacy*): ruído calibrado na resposta de agregações, com
#   garantia matemática independente do que o atacante sabe. É o estado da arte para publicar estatística;
#   custa precisão e exige controle do orçamento de consultas (ε).
# - **Tokenização preservando formato** (FPE, *format-preserving encryption*): o token de um CPF tem cara de
#   CPF — sistemas legados continuam validando. Normalmente é produto (cofre ou HSM), não `uuid()`.
# - **Criptografia determinística** (AES-SIV, ou ECB — este último inseguro para valores longos): permite
#   junção e reabre a análise de frequência. O `aes_encrypt` do Spark aceita `mode => 'ECB'`; evite.
# - **Onde cifrar**: criptografia em repouso do storage (SSE, chave gerenciada pelo cliente) protege contra
#   roubo de disco, não contra quem tem `SELECT`. Cifra por coluna protege contra leitor autorizado da tabela.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Cofre de tokens é um ponto único: vazou o cofre, vazou tudo; perdeu o cofre, perdeu a reversão.
# - Criptografia por coluna quebra filtro, ordenação, estatística de arquivo e compressão daquela coluna.
# - Generalização é irreversível e decidida na escrita: guardou por hora, não há como voltar ao segundo.

# %% [markdown]
# ## 7. Mascaramento parcial 🧪
#
# **O que é** — **Mascarar** é esconder parte do valor **na exibição**, mantendo o suficiente para a tarefa:
# o atendente confere os últimos dígitos, o analista vê o domínio do e-mail. Diferente de pseudonimizar, a
# máscara não serve de chave e não pretende ser irreversível — ela reduz a exposição.
#
# **Por que importa** — A maioria dos acessos a dado pessoal é de gente que não precisa do valor inteiro:
# suporte, QA, análise exploratória, *prompt* de LLM.
#
# **Como funciona** — Funções nativas do Spark (`mask`, `regexp_replace`), sem UDF. Os dados abaixo são
# **fictícios**: e-mails em `example.com` (domínio reservado pela RFC 2606) e CPFs com dígito verificador
# **propositalmente inválido**, para não coincidirem com o CPF de ninguém — a célula confere.

# %%
def cpf_valido(cpf: str) -> bool:
    d = [int(c) for c in cpf if c.isdigit()]
    if len(d) != 11:
        return False
    dv1 = (sum(a * b for a, b in zip(d[:9], range(10, 1, -1), strict=True)) * 10) % 11 % 10
    dv2 = (sum(a * b for a, b in zip(d[:10], range(11, 1, -1), strict=True)) * 10) % 11 % 10
    return d[9] == dv1 and d[10] == dv2


clientes = spark.createDataFrame(
    [
        ("Ana Souza", "ana.souza@example.com", "123.456.789-00", "octocat-ana"),
        ("Bruno Lima", "b.lima@example.com", "98765432199", "brunol"),
        ("Carla Dias", "carla@example.com", "111.222.333-00", "cd"),
    ],
    "nome string, email string, cpf string, login string",
)
print("algum CPF de exemplo é válido?", any(cpf_valido(r.cpf) for r in clientes.collect()), "| controle (CPF de teste clássico 123.456.789-09):", cpf_valido("123.456.789-09"))
clientes.select(
    g.mascara_formato("nome").alias("nome (formato)"),
    "email",
    g.mascara_email("email").alias("email mascarado"),
    "cpf",
    g.mascara_cpf("cpf").alias("cpf mascarado"),
    g.mascara_parcial("login", 2).alias("login (2 finais)"),
).show(truncate=False)

# %% [markdown]
# - `mascara_email`: 1ª letra + domínio. `mascara_cpf`: `***.456.789-**`, o formato das publicações oficiais
#   brasileiras; aceita com e sem pontuação e devolve `***` para o que não reconhece (nunca o valor aberto).
# - `mascara_formato`: troca maiúscula por `X`, minúscula por `x`, dígito por `9` — preserva só a "forma".
# - `mascara_parcial("cd", 2)` devolveu `cd` **inteiro**: valor curto demais para a máscara. É o caso de borda
#   que a função documenta e que o chamador precisa decidir (aqui, para login de 2 letras, usar `mascara_formato`).
#
# **Texto livre** é o caso difícil: o dado pessoal está no meio da frase. `limpar_texto_livre` aplica regex para
# formatos conhecidos — é o que o notebook 12 chama antes de mandar título de issue para um LLM.

# %%
frase = "Falar com ana.souza@example.com, CPF 123.456.789-00, tel (11) 98765-4321, do IP 192.0.2.10. cc @octocat-ana — a Ana Souza aprovou."
print(frase)
print(g.limpar_texto_livre(frase))
print("\nespelho Python da máscara de formato:", g.mascara_formato_py("Ana.Souza99@example.com"))

# %% [markdown]
# O e-mail, o CPF, o telefone, o IP e a menção saíram. **"Ana Souza" ficou**: regex pega formato, não pega nome
# próprio. Para isso existe NER (*named entity recognition* — reconhecimento de entidades), com falso negativo
# inevitável. Texto livre com dado pessoal se trata como rede de proteção, não como garantia — e por isso a
# política do §4 manda **remover** o `payload` para quem não tem acesso aberto.
#
# > 🎤 **Resposta de 30 s:** "Máscara é para exibição: mostra o mínimo para a tarefa — últimos dígitos, domínio
# > do e-mail — e não serve de chave de junção. Faço com função nativa, falhando fechado: o que a função não
# > reconhece sai todo mascarado. E separo os conceitos: máscara reduz exposição, pseudonimização permite
# > análise, e nenhuma das duas é anonimização."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Máscara estática × dinâmica**: estática grava o dado já mascarado (cópia para dev/QA); dinâmica aplica na
#   leitura conforme quem consulta (§8). Ambiente de desenvolvimento pede estática — dado real mascarado em dev
#   ainda é dado pessoal, e dev tem menos controle.
# - **Vazamento por máscara fraca**: 1ª letra + domínio de um domínio corporativo pequeno identifica a pessoa.
#   A máscara certa depende da cardinalidade do que sobra.
# - **Inferência por filtro**: se a coluna mascarada ainda aceita `WHERE cpf = '…'` sobre o valor real, o
#   usuário descobre o valor por tentativa. Column mask do UC aplica a máscara antes do predicado do usuário.
# - **Dado sintético** (gerado, com as mesmas distribuições) é a alternativa para dev quando nem mascarado pode.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Máscara não protege contra quem tem acesso à tabela base — só contra quem lê pela camada mascarada.
# - Mascarar na aplicação (e não no dado) deixa todo acesso por SQL, BI ou notebook de fora.

# %% [markdown]
# ## 8. Visão dinâmica por papel 🧪 · row filter, column mask e ABAC ☁️
#
# **O que é** — Uma **visão dinâmica** (*dynamic view*) decide **em tempo de consulta** o que mostrar, conforme
# quem pergunta. No Databricks a decisão usa funções de identidade (`is_account_group_member('grupo')`,
# `current_user()`). **Row filter** e **column mask** são a evolução: a regra é uma função SQL presa à
# **tabela**, sem view no meio. **ABAC** (*attribute-based access control* — controle por atributo) prende a
# regra à **tag**: toda coluna tagueada como PII é mascarada, em qualquer tabela.
#
# **Por que importa** — Uma tabela só, vários públicos. Sem isso, a saída é manter cópias (`_mascarada`,
# `_completa`), que divergem e dobram o problema do esquecimento.
#
# **Como funciona (local)** — O Spark open source não sabe quem é o usuário (§1), então o papel é **simulado**
# por uma variável de sessão SQL: `DECLARE VARIABLE papel`. `sql_visao_dinamica` gera o `CASE WHEN` de cada
# coluna a partir da mesma `POLITICA_GH_EVENTS` do §4. Primeiro o SQL na forma legível (sem chave):

# %%
print(g.sql_visao_dinamica("eventos_v", "eventos_base", g.POLITICA_GH_EVENTS))

# %% [markdown]
# Esse SQL "legível" usa `sha2` puro — que o §5 acabou de quebrar. Para a view valer, passamos a chave:
# o `CASE` passa a calcular o **HMAC** (a mesma expressão, em SQL). A mesma consulta roda duas vezes, mudando
# só a variável.

# %%
ev.createOrReplaceTempView("eventos_base")
spark.sql("DECLARE OR REPLACE VARIABLE papel STRING DEFAULT 'analista'")
spark.sql(g.sql_visao_dinamica("eventos_v", "eventos_base", g.POLITICA_GH_EVENTS, chave=chave))

ids = ", ".join(f"'{i}'" for i in IDS_TITULAR)
consulta = f"""
SELECT id, type, substring(actor_login, 1, 21) AS actor_login, substring(actor_id, 1, 12) AS actor_id,
       actor_url IS NULL AS sem_url, payload IS NULL AS sem_payload, repo_name
FROM eventos_v WHERE id IN ({ids}) ORDER BY id"""
for papel in ("analista", "dpo"):
    spark.sql(f"SET VAR papel = '{papel}'")
    print(f"papel = {papel}")
    spark.sql(consulta).show(truncate=False)

spark.sql("SET VAR papel = 'analista'")
visto = spark.sql(f"SELECT actor_login FROM eventos_v WHERE id = '{IDS_TITULAR[0]}'").first()[0]
print("pseudônimo da view == HMAC do pipeline (§5):", visto == g.hmac_sha256_py(TITULAR, chave))

# %% [markdown]
# A mesma view, a mesma consulta: o **analista** recebe pseudônimo no lugar do login e do id, e `NULL` em URL e
# payload; o **DPO** recebe o dado aberto. O pseudônimo da view é o mesmo HMAC que o pipeline grava, então o
# analista junta a view com a silver pseudonimizada.
#
# **Agora olhe a coluna `repo_name` na visão do analista.** Ela está aberta (a política diz "interno, sem
# tratamento") e mostra `zzzz-titular-ficticia/…`: o login que a view escondeu em `actor_login` está escrito na
# coluna ao lado. Quanto isso pesa no dado real?

# %%
real = achatar(bronze.where(F.col("_source_file").endswith("-12.json.gz")))
v = real.agg(
    F.count(F.lit(1)).alias("eventos"),
    F.sum(F.col("repo_name").startswith(F.concat("actor_login", F.lit("/"))).cast("int")).alias("no_proprio_repo"),
).first()
print(f"eventos das 12h em que o dono do repositório É o ator: {v.no_proprio_repo:,} de {v.eventos:,} ({v.no_proprio_repo / v.eventos:.1%})")

analista = g.aplicar_politica(ev, g.POLITICA_GH_EVENTS, "analista", chave)
print("colunas entregues ao analista:", analista.columns)

# %% [markdown]
# Em **70,6%** dos eventos, o `repo_name` desfaz a pseudonimização do `actor_login` sem ataque nenhum.
# **Mascarar a coluna não é proteger o dado**: é preciso olhar onde mais o mesmo valor aparece — outra coluna,
# texto livre, nome de arquivo, valor de partição. A correção aqui é tratar o dono do `repo_name` (separar em
# `repo_owner`, pseudonimizado quando for conta pessoal, e `repo`) — fica registrado como limitação conhecida de
# `POLITICA_GH_EVENTS`, que hoje classifica `repo_name` como interno.
#
# `aplicar_politica` é a versão DataFrame da mesma regra (para o pipeline gravar uma cópia tratada): repare que o
# `payload` e as URLs nem aparecem na lista de colunas.
#
# ### No Unity Catalog ☁️ — três gerações do mesmo controle
#
# **(a) Dynamic view** — o `CASE WHEN` acima, com identidade de verdade:
#
# ```sql
# -- ☁️
# CREATE OR REPLACE VIEW prod.silver.gh_events_v AS
# SELECT id, type,
#        CASE WHEN is_account_group_member('pii-leitores') THEN actor_login ELSE actor_pseudo_id END AS actor,
#        created_at
# FROM prod.silver.gh_events
# WHERE is_account_group_member('dados-admin') OR org_login IS NOT NULL;   -- filtro de linha na própria view
# GRANT SELECT ON VIEW prod.silver.gh_events_v TO `analistas`;             -- e NENHUM grant na tabela base
# ```
#
# **(b) Row filter e column mask na tabela** — a regra é uma função SQL do UC, aplicada pelo dono da tabela:
#
# ```sql
# -- ☁️ Column mask: recebe o valor da coluna, devolve o valor ou a máscara (tipo compatível com a coluna).
# CREATE OR REPLACE FUNCTION prod.governanca.mascara_login(login STRING)
#   RETURNS STRING
#   RETURN CASE WHEN is_account_group_member('pii-leitores') THEN login
#               ELSE concat(left(login, 1), '***') END;
#
# ALTER TABLE prod.silver.gh_events ALTER COLUMN actor_login SET MASK prod.governanca.mascara_login;
#
# -- ☁️ Máscara que depende de OUTRA coluna: só mascara contas pessoais, deixa organização aberta.
# CREATE OR REPLACE FUNCTION prod.governanca.mascara_dono(repo_name STRING, org_login STRING)
#   RETURNS STRING
#   RETURN CASE WHEN org_login IS NOT NULL OR is_account_group_member('pii-leitores') THEN repo_name
#               ELSE concat('***/', substring_index(repo_name, '/', -1)) END;
#
# ALTER TABLE prod.silver.gh_events ALTER COLUMN repo_name
#   SET MASK prod.governanca.mascara_dono USING COLUMNS (org_login);
#
# -- ☁️ Row filter: recebe colunas da linha, devolve BOOLEAN. FALSE = a linha não existe para quem consulta.
# CREATE OR REPLACE FUNCTION prod.governanca.filtro_org(org_login STRING)
#   RETURNS BOOLEAN
#   RETURN is_account_group_member('dados-admin')
#       OR EXISTS (SELECT 1 FROM prod.governanca.acesso_por_org a
#                  WHERE a.org_login = org_login AND is_account_group_member(a.grupo));
#
# ALTER TABLE prod.silver.gh_events SET ROW FILTER prod.governanca.filtro_org ON (org_login);
#
# -- ☁️ Retirar
# ALTER TABLE prod.silver.gh_events ALTER COLUMN actor_login DROP MASK;
# ALTER TABLE prod.silver.gh_events DROP ROW FILTER;
# ```
#
# **(c) ABAC: política por tag** — em vez de `ALTER` tabela a tabela, uma política no schema ou no catálogo que
# vale para toda coluna com a tag governada, inclusive nas tabelas que ainda não existem:
#
# ```sql
# -- ☁️ Toda coluna com a tag governada pii=login, em qualquer tabela do catálogo prod, é mascarada
# --    para todos — exceto o grupo de leitura de PII e o service principal do pipeline.
# CREATE POLICY mascara_pii_login
# ON CATALOG prod
# COMMENT 'Mascara logins para quem não é leitor de PII'
# COLUMN MASK prod.governanca.mascara_login
# TO `account users` EXCEPT `pii-leitores`, `sp-pipeline-silver`
# FOR TABLES
# MATCH COLUMNS has_tag_value('pii', 'login') AS col_login
# ON COLUMN col_login;
#
# -- ☁️ Filtro de linha em toda tabela tagueada como sensível que tenha uma coluna tagueada como org
# CREATE POLICY filtra_por_org
# ON SCHEMA prod.silver
# ROW FILTER prod.governanca.filtro_org
# TO `analistas`
# FOR TABLES
# WHEN has_tag_value('sensibilidade', 'alta')
# MATCH COLUMNS has_tag('org') AS org
# USING COLUMNS (org);
#
# SHOW EFFECTIVE POLICIES ON TABLE prod.silver.gh_events;
# ```
#
# **Estado do ABAC (conferido na documentação do Azure Databricks de 29/09/2026):** políticas de row filter e de
# column mask em catálogo, schema e tabela aparecem sem aviso de preview e são o caminho **recomendado** pela
# doc para regra que vale em muitas tabelas. Continuam em **Beta**: política no nível do **metastore**,
# políticas `DENY` e política sobre **views**. Exige serverless ou Databricks Runtime **16.4+** (em compute
# *dedicated*, com a filtragem fina habilitada); runtime mais antigo **não lê** tabela protegida por ABAC.
# Confira a página antes da entrevista — este é o recurso que mais mudou em 2025–2026.
#
# | | Dynamic view | Row filter / column mask na tabela | Política ABAC |
# |---|---|---|---|
# | Onde a regra mora | no SQL da view | função + `ALTER TABLE`, tabela a tabela | política no catálogo/schema, casada por **tag** |
# | Quem administra | dono da view | dono da tabela | dono do catálogo/schema (o dono da tabela não remove) |
# | Tabela nova | precisa de view nova | precisa de `ALTER` novo | coberta quando recebe a tag |
# | Nome que o usuário consulta | o da view (outro objeto) | o da própria tabela | o da própria tabela |
# | Melhor uso | expor um recorte curado, com join/transformação | regra específica de uma tabela | regra corporativa ("toda PII é mascarada") |
#
# > 🎤 **Resposta de 30 s:** "São três gerações do mesmo controle. Dynamic view: um CASE WHEN com
# > is_account_group_member, e o usuário só tem grant na view. Row filter e column mask: funções SQL presas à
# > tabela com ALTER TABLE — o usuário consulta a tabela e a regra vai junto. ABAC: política no catálogo ou no
# > schema, casada por tag governada — taguei a coluna como PII e a máscara vale em qualquer tabela, inclusive
# > nas futuras. Para regra corporativa eu vou de ABAC; a identidade do pipeline entra no EXCEPT, senão a silver
# > é gravada já mascarada."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **O pipeline também é "usuário"**: se o service principal que lê a bronze cair na máscara, ele grava a
#   silver **mascarada para sempre**. O mesmo vale para materialized views e streaming tables, que são
#   atualizadas com a identidade do dono do pipeline. Daí o `EXCEPT`.
# - **Conflito**: só **um** row filter por tabela e **uma** máscara por coluna podem valer para um usuário. Duas
#   políticas ABAC diferentes casando na mesma coluna → a consulta **falha** (falha fechada, não escolhe uma).
# - **Limitações que derrubam desenho** (doc de 24/09/2026): tabela com row filter/column mask não aceita leitura
#   **por caminho** nem pelas APIs do Delta; não pode ser clonada (deep/shallow); *time travel* não é suportado
#   com filtro/máscara de tabela (com ABAC está em Beta); não pode ser compartilhada por Delta Sharing quando o
#   controle é de tabela (com ABAC pode, se o dono do share estiver no `EXCEPT`); em compute *dedicated* exige
#   DBR 15.4+ e a filtragem acontece no serverless.
# - **Performance**: a função roda por linha, e o otimizador não pode empurrar filtro para antes da máscara se
#   isso vazar informação. Função simples (`CASE`), determinística, sem subconsulta quando der, SQL em vez de
#   Python. A tabela de mapeamento do `filtro_org` acima é o padrão que mais custa — vale medir.
# - **Tipos**: se a coluna é `STRING` e o parâmetro da função é `INT`, o cast implícito com ANSI desligado vira
#   `NULL` em silêncio e o filtro passa a devolver linha errada. Tipo do parâmetro = tipo da coluna.
# - **Por que a máscara não faz HMAC**: máscara é para exibição. O pseudônimo de junção é **materializado** pelo
#   pipeline (`actor_pseudo_id`, §5) — calculado uma vez, com a chave do Key Vault, e igual para todo mundo.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - A simulação local por variável de sessão **não é controle de acesso**: qualquer um faz `SET VAR papel = 'dpo'`.
#   Ela demonstra a mecânica; segurança só com identidade verificada pela plataforma.
# - Colocar a chave como literal na view (como aqui) expõe a chave a quem lê a definição — aceitável só em demo.
# - ABAC depende de a tag estar certa: coluna de PII sem tag fica aberta. Tag errada é o novo grant errado.

# %% [markdown]
# ## 9. Lineage, auditoria, Delta Sharing e isolamento de ambientes ☁️
#
# **O que é**
# - **Lineage** (*linhagem*): o grafo de quem lê e quem escreve cada tabela e cada **coluna**, capturado
#   automaticamente pelo UC a partir das consultas executadas (SQL, DataFrame, pipelines, jobs).
# - **Auditoria**: o registro de cada ação na plataforma — quem, quando, de onde, sobre qual objeto.
# - **Delta Sharing**: protocolo aberto para compartilhar tabelas com outra organização **sem copiar** o dado.
# - **Isolamento de ambientes**: como separar dev, homologação e produção.
#
# **Por que importa** — Para a LGPD, lineage responde "para onde foi o dado deste titular?" (a lista de tabelas
# em que o esquecimento do §13 precisa rodar) e auditoria responde "quem acessou?" (a pergunta da investigação
# de incidente).
#
# **Como funciona** — As duas coisas ficam em **system tables** (tabelas de sistema, no catálogo `system`),
# consultáveis por SQL. Nada disso existe no Spark local — o que o open source oferece é o `DESCRIBE HISTORY`
# do Delta, que registra **escritas** por tabela e não diz quem **leu**.

# %%
(
    spark.sql(f"DESCRIBE HISTORY delta.`{EV}`")
    .select("version", "operation", F.col("userName").alias("user"), F.substring(F.col("operationParameters").cast("string"), 1, 60).alias("parametros (início)"))
    .orderBy("version")
    .show(4, truncate=False)
)

# %% [markdown]
# O histórico local tem a operação e os parâmetros, mas `user` vem vazio (não há identidade) e leitura não
# aparece em lugar nenhum. No Databricks:
#
# ```sql
# -- ☁️ Lineage de tabela: tudo que foi gravado A PARTIR da bronze (para onde o dado pessoal se espalhou)
# SELECT DISTINCT target_table_full_name, entity_type
# FROM system.access.table_lineage
# WHERE source_table_full_name = 'prod.bronze.gh_events'
#   AND target_table_full_name IS NOT NULL;
#
# -- ☁️ Lineage de coluna: quais colunas derivam de actor.login
# SELECT DISTINCT target_table_full_name, target_column_name
# FROM system.access.column_lineage
# WHERE source_table_full_name = 'prod.bronze.gh_events' AND source_column_name = 'actor';
#
# -- ☁️ Auditoria: quem acessou a tabela nos últimos 30 dias
# SELECT event_time, user_identity.email, action_name, source_ip_address
# FROM system.access.audit
# WHERE service_name = 'unityCatalog'
#   AND action_name IN ('getTable', 'generateTemporaryTableCredential')
#   AND request_params.full_name_arg = 'prod.silver.gh_events'
#   AND event_date >= current_date() - INTERVAL 30 DAYS
# ORDER BY event_time DESC;
#
# -- ☁️ Auditoria: quem mudou permissão, tag ou política
# SELECT event_time, user_identity.email, action_name, request_params
# FROM system.access.audit
# WHERE service_name = 'unityCatalog'
#   AND action_name IN ('updatePermissions', 'createEntityTagAssignment', 'deleteEntityTagAssignment',
#                       'createPolicy', 'deletePolicy');
# ```
#
# (Os nomes de `action_name` variam por serviço e por versão; a referência de *audit logs* é a fonte — confira
# antes de montar alerta em cima deles.)
#
# **Delta Sharing (conceito)** — O provedor cria um **share** (coleção somente leitura de tabelas), um
# **recipient** (o destinatário) e concede o share ao recipient. O destinatário lê os arquivos direto do storage
# do provedor, com URLs assinadas de vida curta: sem cópia, sem pipeline de exportação. Dois modos:
# *Databricks-to-Databricks* (o destinatário tem UC; sem token, com auditoria dos dois lados) e *open sharing*
# (qualquer cliente — pandas, Power BI, Spark — com token ou federação OIDC).
#
# ```sql
# -- ☁️
# CREATE SHARE parceiros_oss COMMENT 'Agregados diários, sem dado pessoal';
# ALTER SHARE parceiros_oss ADD TABLE prod.gold.fct_eventos_diarios;
# CREATE RECIPIENT universidade_x USING ID 'azure:brazilsouth:<id-do-metastore-do-destinatario>';
# GRANT SELECT ON SHARE parceiros_oss TO RECIPIENT universidade_x;
# ```
#
# **Nome:** na documentação do Azure Databricks de setembro de 2026 o recurso aparece como **OpenSharing**
# (o protocolo e o projeto open source foram renomeados; o repositório continua em `go.delta.io/sharing`). Em
# entrevista os dois nomes aparecem — é a mesma coisa. Para LGPD, compartilhar é **uso compartilhado** de dado
# (o titular tem direito de saber com quem, art. 18 VII): compartilhe a gold agregada, não a bronze.
#
# **Isolamento de ambientes** — duas decisões independentes:
#
# | | Catálogo por ambiente (`dev`, `hml`, `prod` no mesmo metastore) | Workspace por ambiente |
# |---|---|---|
# | O que isola | **dado e permissão** | **computação, rede, usuários, jobs, segredos** |
# | Como | `GRANT` por catálogo; *managed location* própria (contêiner ou conta de storage separada) | um workspace para cada, com VNet e políticas próprias |
# | Código | o mesmo; o catálogo é parâmetro (`${var.catalog}` no bundle — nb 13) | deploy por *target* do bundle |
# | Risco se for o único | job de dev com permissão errada alcança `prod` | sem catálogo separado, todos os workspaces veem os mesmos dados |
#
# O desenho usual é **os dois**: um workspace por ambiente **e** um catálogo por ambiente, com
# *workspace-catalog binding* — o catálogo `prod` só é acessível a partir do workspace de produção, então um
# notebook no workspace de dev não lê produção nem que alguém tenha o `GRANT`.
#
# ```sql
# -- ☁️ o binding em si é feito pelo Catalog Explorer, API ou Terraform (databricks_workspace_binding)
# ALTER CATALOG prod SET ISOLATION MODE ISOLATED;   -- deixa de ser acessível a todos os workspaces do metastore
# ```
#
# > 🎤 **Resposta de 30 s:** "Lineage e auditoria são system tables: system.access.table_lineage e
# > column_lineage me dizem para onde o dado foi, system.access.audit diz quem acessou e quem mudou permissão.
# > É com lineage que eu monto a lista de tabelas de um pedido de eliminação. Ambiente eu isolo em dois eixos:
# > workspace por ambiente para computação e rede, catálogo por ambiente para dado, com binding para o catálogo
# > de produção só existir no workspace de produção. E compartilhamento externo é Delta Sharing, sem cópia, só
# > de dado agregado."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Lineage tem buracos**: só captura o que passa pelo UC. Leitura por caminho, `collect()` seguido de escrita
#   em outro sistema, exportação para planilha — nada disso aparece. Lineage é ponto de partida do inventário,
#   não a prova de completude.
# - **Retenção das system tables**: é limitada (da ordem de um ano); se a política da empresa pede mais, exporte
#   para uma tabela própria. E o próprio log de auditoria tem dado pessoal (e-mail, IP) — tem de entrar no
#   inventário e ter prazo.
# - **Verbose audit logs**: opção de workspace que registra o **texto** de cada comando de notebook. Útil para
#   investigação; cuidado com o que as pessoas colam em células.
# - **Catálogo de dev com dado de prod**: o atalho mais comum e o mais caro. Dev recebe amostra **mascarada de
#   forma estática** ou dado sintético — nunca `DEEP CLONE` de prod.
# - **Share e esquecimento**: o destinatário lê os arquivos atuais; depois do `DELETE` + `VACUUM` no provedor, o
#   dado some para ele também. Se ele **copiou**, o esquecimento depende de contrato — por isso o art. 18 §6º
#   manda o controlador avisar os agentes com quem compartilhou.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Um workspace por ambiente multiplica administração (rede, políticas, grupos); em time pequeno, um workspace
#   com catálogos separados e service principals distintos é um começo defensável — desde que dito como dívida.
# - System tables custam consulta e têm atraso de minutos a horas: servem para investigação e relatório, não
#   para bloquear acesso em tempo real.

# %% [markdown]
# ## 10. LGPD para engenheiro de dados
#
# **O que é** — A Lei Geral de Proteção de Dados (Lei 13.709/2018) regula o **tratamento** de dado pessoal:
# coletar, armazenar, transformar, compartilhar e apagar são todos "tratamento". O pipeline inteiro está dentro.
#
# **Por que importa** — As decisões de engenharia (o que a bronze guarda, por quanto tempo, quem lê, como apaga)
# **são** o cumprimento ou o descumprimento da lei. O jurídico define a regra; quem a torna verdadeira é o
# pipeline.
#
# **Como funciona — o vocabulário mínimo**
#
# | Conceito | Definição (art. 5º) | No GH Archive |
# |---|---|---|
# | **Dado pessoal** | informação relacionada a pessoa natural **identificada ou identificável** | `actor.login`, `actor.id`, e-mail em mensagem de commit |
# | **Dado pessoal sensível** | origem racial ou étnica, convicção religiosa, opinião política, filiação sindical ou a organização religiosa/filosófica/política, saúde, vida sexual, dado genético ou biométrico | não há coluna dedicada — mas pode aparecer em **texto livre** (`payload`) |
# | **Dado anonimizado** | titular **não pode ser identificado**, considerados os meios técnicos razoáveis | contagem de eventos por tipo e hora |
# | **Pseudonimizado** (art. 13 §4º) | só identifica com informação adicional mantida em separado | `actor_pseudo_id` (HMAC) — **continua pessoal** |
# | **Controlador / operador** | quem decide o tratamento / quem trata em nome do controlador | a empresa / o fornecedor de nuvem |
# | **Encarregado** (DPO) | canal entre controlador, titulares e a ANPD (art. 41) | §14 |
#
# **A nuance que mais cai: login do GitHub é público e AINDA é dado pessoal.** "Público" descreve o acesso;
# "pessoal" descreve a relação com uma pessoa. A lei trata disso no art. 7º: o §3º manda considerar a
# **finalidade, a boa-fé e o interesse público** que justificaram a publicação, e o §4º dispensa o consentimento
# para dados tornados manifestamente públicos pelo titular — **resguardados os direitos do titular e os
# princípios da lei**. Ou seja: ninguém precisa pedir consentimento para analisar atividade pública de código
# aberto; e ninguém pode, por isso, montar perfil individual para outra finalidade, guardar para sempre ou
# ignorar um pedido de eliminação. Os §§5–§8 deste notebook mostraram por quê, na prática: o dado "público" é
# o que torna o ataque de dicionário trivial.
#
# **Anonimizado sai da lei — se for de verdade.** O art. 12 diz que dado anonimizado não é dado pessoal,
# **salvo se a anonimização puder ser revertida** com esforços razoáveis. Hash sem sal não passa nesse teste
# (§5); tirar o nome e deixar os quase-identificadores também não (§6).
#
# **Bases legais (art. 7º)** — todo tratamento precisa de **uma**. São dez: consentimento; obrigação legal ou
# regulatória; políticas públicas; estudos por órgão de pesquisa; execução de contrato; exercício regular de
# direitos em processo; proteção da vida; tutela da saúde; **legítimo interesse**; proteção do crédito.
# Consentimento não é a base "padrão" nem a mais segura — pode ser revogado a qualquer momento. Para análise de
# atividade pública, a base usual é o legítimo interesse, que exige teste de balanceamento documentado. Para o
# engenheiro, a consequência prática: **a base legal é metadado da tabela** (qual finalidade, qual base, qual
# prazo) — é ela que diz se um pedido de eliminação procede.
#
# **Princípios que viram requisito de pipeline (art. 6º)**
#
# | Princípio | O que significa | Como vira engenharia |
# |---|---|---|
# | **Finalidade** e adequação | usar para o propósito informado | finalidade registrada por tabela; nova finalidade = nova avaliação |
# | **Necessidade** (minimização) | só o dado necessário | não levar coluna pessoal para a camada que não precisa dela — a gold deste projeto não tem login; §8: o analista recebe a tabela sem `payload` |
# | **Qualidade** | dado exato e atualizado | direito de correção → `MERGE`, SCD (nb 05) |
# | **Segurança** e prevenção | proteger contra acesso indevido | grants, máscara, chave no cofre, auditoria (§3, §5, §8, §9) |
# | **Responsabilização** (*accountability*) | conseguir **demonstrar** que cumpre | política em código, testes, histórico do Delta, logs de auditoria |
#
# **Retenção** — O tratamento termina quando a finalidade é alcançada ou o prazo acaba (art. 15), e o dado deve
# ser eliminado, salvo as hipóteses de conservação do art. 16 (obrigação legal, estudo por órgão de pesquisa,
# transferência a terceiro, uso exclusivo do controlador com o dado anonimizado). Em Delta, retenção é um job:
# `DELETE WHERE event_date < current_date() - 90` **mais** `VACUUM` — pelo mesmo motivo do §11.
#
# **Direitos do titular (art. 18)** — confirmação da existência de tratamento; acesso; correção; anonimização,
# bloqueio ou eliminação de dados desnecessários ou excessivos; portabilidade; **eliminação** dos dados tratados
# com consentimento; informação sobre com quem o dado foi compartilhado; informação sobre a possibilidade de
# não consentir; revogação do consentimento. Cada um vira uma capacidade do lakehouse:
#
# | Direito | Capacidade técnica |
# |---|---|
# | Acesso / confirmação | achar **todas** as linhas do titular, em todas as tabelas → classificação (§4) + lineage (§9) + chave de busca estável |
# | Correção | `MERGE` na origem e reprocessamento das derivadas |
# | Portabilidade | exportar em formato estruturado e interoperável |
# | Eliminação | §11–§13 |
# | Informação de compartilhamento | inventário de shares e exportações (§9) |
#
# > 🎤 **Resposta de 30 s:** "Dado pessoal é o que identifica ou torna identificável uma pessoa — login público
# > do GitHub incluído: ser público dispensa o consentimento, não tira o dado da lei. Sensível é a lista fechada
# > do artigo 5º. Anonimizado sai do escopo só se não for reversível com esforço razoável; pseudonimizado
# > continua dentro. Todo tratamento precisa de uma das dez bases legais, e os princípios viram requisito de
# > pipeline: minimização é não propagar coluna pessoal, retenção é DELETE mais VACUUM agendado, e os direitos do
# > titular exigem que eu saiba achar e apagar uma pessoa em todas as camadas."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **LGPD × GDPR**: estrutura parecida (a LGPD foi inspirada no regulamento europeu). Diferenças que aparecem:
#   a LGPD tem dez bases legais (inclui proteção do crédito); o GDPR fixa um mês para responder ao titular,
#   enquanto a LGPD fixa prazo expresso para acesso/confirmação (art. 19: formato simplificado de imediato, ou
#   declaração completa em até 15 dias) e deixa os demais para regulamentação da ANPD.
# - **Incidente de segurança** (art. 48): comunicar à ANPD e aos titulares quando houver risco ou dano
#   relevante; o regulamento da ANPD (Resolução CD/ANPD nº 15/2024) fixa três dias úteis a partir do
#   conhecimento. Para o engenheiro: saber dizer **quais** titulares e **quais** dados — de novo, classificação
#   e auditoria.
# - **Relatório de impacto** (RIPD, art. 38) e **registro das operações** (art. 37): o engenheiro é fonte — quais
#   tabelas, quais colunas, de onde vêm, para onde vão, quanto tempo ficam.
# - **Transferência internacional** (art. 33): a região do workspace e do storage é decisão jurídica também.
#   `brazilsouth` não é detalhe de latência.
# - **Dado de criança e adolescente** (art. 14): regime mais estrito; plataforma aberta pode ter.
# - **Privacy by design** (art. 46 §2º): as medidas valem desde a concepção — o argumento para classificar na
#   bronze e não "depois".
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Minimizar na bronze (não guardar o `payload` bruto) conflita com o princípio de engenharia de "bronze é o
#   dado como veio, para reprocessar". A saída comum: bronze com retenção **curta** e acesso restrito; silver já
#   tratada com retenção longa.
# - "Anonimizar para guardar para sempre" só vale se a anonimização resistir ao art. 12 — medir (k, §6), não supor.

# %% [markdown]
# ## 11. Direito à eliminação na prática: `DELETE` não apaga 🧪
#
# **O que é** — O titular pede a eliminação. Você roda `DELETE FROM tabela WHERE actor_login = '…'`, a consulta
# seguinte devolve zero linhas, e o chamado é fechado. **O dado continua no disco.**
#
# **Por que importa** — O Delta é um formato de **arquivos imutáveis com log de transações**. `DELETE` não edita
# arquivo: grava arquivos novos sem as linhas e registra no log que os antigos saíram da versão atual. Os
# antigos ficam — é isso que torna possível o *time travel* (consultar versões anteriores) e é isso que torna o
# `DELETE` insuficiente para a LGPD.
#
# **Como funciona**
#
# ```text
# versão 0 (escrita)        versão 1 (DELETE)                    depois do VACUUM
#   a.parquet  ← titular      a.parquet   (removido do log,        a2.parquet
#   b.parquet                              AINDA NO DISCO)         b.parquet
#                             a2.parquet  (a.parquet sem as
#                                          linhas do titular)
#                             b.parquet
# ```
#
# A tabela desta seção é uma cópia dos eventos de demonstração (sem `payload`), em 4 arquivos, **sem** deletion
# vectors (o padrão do Delta open source; o caso com deletion vectors é o §12).

# %%
COLS = ["id", "type", "actor_id", "actor_login", "repo_name", "created_at"]
COW = str(DEMO / "apagar_cow")
ev.select(*COLS).repartition(4).write.format("delta").save(COW)


def parquets(caminho: str) -> list[Path]:
    return sorted(Path(caminho).glob("*.parquet"))


def estado(caminho: str, rotulo: str) -> None:
    atual = spark.read.format("delta").load(caminho)
    print(
        f"{rotulo:<34} versão atual: {atual.where(do_titular).count():>4} linhas | "
        f"parquet no disco: {len(parquets(caminho))} arquivos, {sum(p.stat().st_size for p in parquets(caminho)) / 1e6:.1f} MB, "
        f"com {g.contar_no_parquet_bruto(spark, caminho, 'actor_login', TITULAR)} linhas do titular"
    )


estado(COW, "antes")
spark.sql(f"DELETE FROM delta.`{COW}` WHERE actor_login = '{TITULAR}'")
estado(COW, "depois do DELETE")

h = spark.sql(f"DESCRIBE HISTORY delta.`{COW}` LIMIT 1").first()
V_DELETE = h["version"]
m = h["operationMetrics"]
print(f"\nmétricas do DELETE (versão {V_DELETE}): apagou {m['numDeletedRows']} linhas, removeu {m['numRemovedFiles']} arquivos do log, "
      f"gravou {m['numAddedFiles']} novos, copiou {int(m['numCopiedRows']):,} linhas que não mudaram")

# %% [markdown]
# A versão atual não tem mais o titular, mas a pasta passou de 4 para 8 arquivos e a leitura **direta dos
# parquet** (ignorando o `_delta_log`) ainda encontra as mesmas linhas do titular. Repare também no custo:
# para apagar essas linhas, o `DELETE` reescreveu os 4 arquivos e copiou as outras 92 mil, que não mudaram — é
# o **copy-on-write**, e o §12 mostra a alternativa.
#
# Não é preciso ler parquet na mão para recuperar o dado. O **time travel** faz isso com uma opção:

# %%
antes = spark.read.format("delta").option("versionAsOf", V_DELETE - 1).load(COW)
print(f"time travel para a versão {V_DELETE - 1}: {antes.where(do_titular).count()} linhas do titular")
antes.where(do_titular).select("id", "type", "actor_login", "repo_name").orderBy("id").show(3, truncate=False)

print("RESTORE também traria tudo de volta: RESTORE TABLE … TO VERSION AS OF", V_DELETE - 1)

# %% [markdown]
# Quem remove os arquivos antigos é o **`VACUUM`**: ele apaga do disco os arquivos que não pertencem à versão
# atual **e** são mais velhos que a retenção — por padrão, **7 dias**. Rodado agora, o `VACUUM` padrão não
# apaga nada (os arquivos têm segundos de idade):

# %%
a_remover = spark.sql(f"VACUUM delta.`{COW}` DRY RUN").count()
print(f"VACUUM padrão (retenção de 7 dias), DRY RUN: {a_remover} arquivos seriam removidos")

try:
    spark.sql(f"VACUUM delta.`{COW}` RETAIN 0 HOURS")
except Exception as e:  # noqa: BLE001
    print(f"\nVACUUM RETAIN 0 HOURS → {type(e).__name__}:")
    print("  " + " ".join(str(e).split())[:330] + "…")

# %% [markdown]
# O Delta **recusa** retenção menor que 7 dias. A trava existe por três motivos:
#
# 1. **Leitores em andamento**: uma consulta longa que começou na versão antiga ainda vai abrir aqueles arquivos;
#    apagá-los a derruba com `FileNotFoundException`.
# 2. **Streams e jobs atrasados**: um stream que lê a tabela e ficou parado alguns dias precisa das versões
#    intermediárias.
# 3. **Escritas concorrentes**: arquivos de uma transação ainda **não commitada** não estão no log — para o
#    `VACUUM` parecem lixo. Com retenção zero ele pode apagar o arquivo de uma escrita em andamento e
#    **corromper a tabela**.
#
# Aqui a tabela é só deste notebook, ninguém mais lê ou escreve: `vacuum_imediato` desliga a checagem
# (`spark.databricks.delta.retentionDurationCheck.enabled = false`), roda `VACUUM … RETAIN 0 HOURS` e **religa**
# a checagem num `finally`. **Isso é demonstração.** Em produção a retenção é curta o suficiente para cumprir o
# prazo (por exemplo, 7 dias) e nunca zero.

# %%
g.vacuum_imediato(spark, COW)
estado(COW, "depois do VACUUM RETAIN 0 HOURS")
print("checagem de retenção religada:", spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled"))

try:
    spark.read.format("delta").option("versionAsOf", V_DELETE - 1).load(COW).where(do_titular).count()
except Exception as e:  # noqa: BLE001
    print(f"\ntime travel para a versão {V_DELETE - 1} → {type(e).__name__}:")
    print("  " + " ".join(str(e).split())[:230] + "…")

print("\nhistórico (o log continua lá):")
spark.sql(f"DESCRIBE HISTORY delta.`{COW}`").select("version", "operation").orderBy("version").show(truncate=False)

# %% [markdown]
# Agora sim: a pasta voltou a 4 arquivos, a leitura direta dos parquet não encontra o titular e o time travel
# para a versão anterior **falha** — os arquivos que ela referencia não existem mais. O histórico ainda lista a
# versão (o log não foi apagado), mas ela não é mais legível.
#
# Esse é o preço: **eliminar de verdade e manter time travel longo são objetivos opostos.** A retenção do
# `VACUUM` é, ao mesmo tempo, a janela de recuperação de erro e o prazo máximo que um dado "apagado" sobrevive.
#
# > 🎤 **Resposta de 30 s:** "DELETE em Delta é lógico: grava arquivos novos e tira os antigos do log, mas eles
# > ficam no disco e o time travel devolve o dado. Quem apaga fisicamente é o VACUUM, que só remove o que saiu da
# > versão atual e é mais velho que a retenção — sete dias por padrão. Então eliminação é DELETE mais VACUUM,
# > com a retenção alinhada ao prazo legal. Retenção zero eu só uso em demonstração: ela pode apagar arquivo de
# > escrita em andamento e derruba leitores. E VACUUM mata o time travel anterior a ele — é uma troca consciente."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Propriedades de retenção**: `delta.deletedFileRetentionDuration` (padrão 7 dias) é o que o `VACUUM` usa;
#   `delta.logRetentionDuration` (padrão 30 dias) é por quanto tempo o **log** fica. Time travel exige os dois:
#   o log da versão **e** os arquivos. No Databricks Runtime 18.0+, a documentação manda controlar a retenção
#   pela propriedade da tabela, não pelo `RETAIN n HOURS`.
# - **`VACUUM` não é automático** no Delta open source nem em tabela externa: sem job agendado, o dado "apagado"
#   fica para sempre. Em tabela **gerenciada** do UC, a *predictive optimization* roda `VACUUM` sozinha.
# - **`VACUUM LITE`** (Delta recente; no Databricks, 16.4+): usa o log em vez de listar a pasta — mais rápido,
#   mas não remove arquivo que nunca esteve no log (resto de transação abortada). Para eliminação, `FULL`.
# - **Cache**: com *disk cache* ligado, um cluster pode continuar respondendo com o conteúdo de um parquet já
#   apagado até reiniciar. "Sumiu do storage" e "sumiu do cluster" são momentos diferentes.
# - **Change Data Feed**: a pasta `_change_data` guarda as linhas alteradas — inclusive as apagadas (`delete`
#   pré-imagem). O `VACUUM` também a limpa, com a mesma retenção.
# - **Custo do `VACUUM`**: listagem em paralelo nos executors, remoção no driver. Tabela com milhões de arquivos
#   pede driver maior, não mais workers.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Retenção curta = menos tempo para desfazer um `DELETE` errado ou um `MERGE` com bug. Compense com backup
#   **que também tenha prazo** (§13), não com retenção longa.
# - `DELETE` por titular em tabela grande e sem layout favorável reescreve muito arquivo. Particionar ou
#   clusterizar pela chave de busca do titular (ou por um *bucket* do pseudônimo) é decisão de modelagem (nb 09).

# %% [markdown]
# ## 12. Deletion vectors: quando nem o `VACUUM` resolve 🧪
#
# **O que é** — **Deletion vector (DV)** é um arquivo pequeno (`.bin`) que marca **quais linhas** de um parquet
# estão apagadas. Com DVs, `DELETE`/`UPDATE`/`MERGE` deixam de reescrever o arquivo inteiro: gravam só o
# vetor (*merge-on-read*). O leitor abre o parquet e pula as linhas marcadas.
#
# **Por que importa** — DVs tornam o `DELETE` muito mais barato — e mudam a história do §11: o parquet original,
# **com o titular dentro**, continua sendo um arquivo **da versão atual**. O `VACUUM` só remove arquivo que saiu
# da versão atual. Logo, `DELETE` + `VACUUM` **não apaga o dado**. No Databricks, tabelas novas nascem com DVs
# ligados por padrão — este é o caso normal em produção, não a exceção.
#
# **Como funciona**
#
# ```text
# DELETE com DV                     REORG … APPLY (PURGE)             VACUUM
#   a.parquet  ← titular (ainda!)     a.parquet   (saiu do log)         a2.parquet
#   a.dv.bin   "pule as linhas 7,9"   a2.parquet  (sem o titular)       b.parquet
#   b.parquet                         b.parquet
# ```
#
# A mesma tabela do §11, agora criada com `delta.enableDeletionVectors = true`.

# %%
DV = str(DEMO / "apagar_dv")
ev.select(*COLS).repartition(4).write.format("delta").option("delta.enableDeletionVectors", "true").save(DV)

estado(DV, "antes")
spark.sql(f"DELETE FROM delta.`{DV}` WHERE actor_login = '{TITULAR}'")
m = spark.sql(f"DESCRIBE HISTORY delta.`{DV}` LIMIT 1").first()["operationMetrics"]
estado(DV, "depois do DELETE")
print(f"\nmétricas do DELETE: apagou {m['numDeletedRows']} linhas com {m['numDeletionVectorsAdded']} deletion vectors; "
      f"arquivos novos: {m['numAddedFiles']}; linhas copiadas: {m['numCopiedRows']}")
print("arquivos .bin na pasta:", [f"{p.name[:28]}… ({p.stat().st_size} bytes)" for p in Path(DV).glob("deletion_vector*.bin")])

# %% [markdown]
# Compare com o §11: **nenhum** arquivo de dados novo, **zero** linhas copiadas — só um `.bin` de menos de 1 KB.
# A pasta continua com os mesmos 4 parquet, e eles ainda têm todas as linhas do titular.
#
# Agora o `VACUUM` com retenção zero — o mesmo que resolveu o §11:

# %%
g.vacuum_imediato(spark, DV)
estado(DV, "depois do VACUUM (sem REORG)")

# %% [markdown]
# **Nada mudou.** O `VACUUM` não tinha o que remover: os 4 parquet são arquivos válidos da versão atual. Um
# time que só conhece "DELETE + VACUUM" fecha o chamado aqui, com o dado no disco e sem time travel envolvido —
# basta ler o parquet.
#
# `REORG TABLE … APPLY (PURGE)` reescreve os arquivos que têm linhas marcadas, **materializando** a exclusão.
# Depois dele os arquivos antigos saem da versão atual, e aí o `VACUUM` os alcança.

# %%
spark.sql(f"REORG TABLE delta.`{DV}` APPLY (PURGE)")
m = spark.sql(f"DESCRIBE HISTORY delta.`{DV}` LIMIT 1").first()
print(f"REORG → operação '{m['operation']}': removeu {m['operationMetrics']['numRemovedFiles']} arquivos do log, gravou {m['operationMetrics']['numAddedFiles']}")
estado(DV, "depois do REORG PURGE")
g.vacuum_imediato(spark, DV)
estado(DV, "depois do REORG PURGE + VACUUM")
print("arquivos .bin restantes:", len(list(Path(DV).glob("deletion_vector*.bin"))))

# %% [markdown]
# A sequência completa para eliminar numa tabela com deletion vectors é **`DELETE` → `REORG … APPLY (PURGE)` →
# `VACUUM`** (respeitada a retenção). É o que `governance.esquecer_titular` embrulha (as duas primeiras etapas) e
# o que `tests/test_governance.py` verifica de ponta a ponta, lendo o parquet bruto.
#
# > 🎤 **Resposta de 30 s:** "Com deletion vectors o DELETE não reescreve o parquet: grava um vetor dizendo
# > quais linhas pular. É ótimo para performance, mas o arquivo com o dado continua fazendo parte da versão
# > atual, então o VACUUM não o remove. Para eliminação de verdade eu preciso de REORG TABLE APPLY PURGE, que
# > reescreve os arquivos sem as linhas marcadas, e só depois o VACUUM apaga os originais. Como no Databricks
# > tabela nova já nasce com deletion vectors, esse é o fluxo padrão de um pedido de esquecimento."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **`OPTIMIZE` também materializa** DVs dos arquivos que ele reescrever — mas só mexe nos arquivos que escolhe
#   compactar. `REORG … APPLY (PURGE)` garante **todos** os arquivos com exclusão pendente. Em tabela gerenciada
#   com *predictive optimization* a materialização acaba acontecendo; "acaba acontecendo" não é prazo legal.
# - **`REORG` aceita `WHERE`** em coluna de partição, para limitar a reescrita.
# - **Outro soft delete**: `ALTER TABLE … DROP COLUMN` com *column mapping* só tira a coluna do schema — os
#   valores continuam nos parquet. O mesmo `REORG … APPLY (PURGE)` + `VACUUM` é o que remove fisicamente. Dropar
#   a coluna de CPF não apaga os CPFs.
# - **Compatibilidade**: DV é *table feature* de leitura e escrita — leitor antigo (ou conector que não entende
#   DV) não abre a tabela. Vale checar antes de ligar numa tabela lida por ferramenta externa.
# - **Quando o DV piora**: muitas exclusões pequenas acumuladas degradam a leitura (o leitor aplica o vetor em
#   todo scan) até alguém reescrever. A manutenção deixa de ser opcional.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - DV troca custo de escrita por custo de leitura e de manutenção. Tabela com muito `DELETE`/`MERGE` pontual:
#   liga. Tabela *append-only*: indiferente.
# - `REORG PURGE` reescreve arquivo — é exatamente o custo que o DV adiou. Para pedidos de eliminação, agrupe
#   (um lote por dia ou semana, dentro do prazo) em vez de um `REORG` por titular.

# %% [markdown]
# ## 13. O que sobra depois do `VACUUM`: log, outras colunas, camadas, backups — e crypto-shredding 🧪
#
# **O que é** — A eliminação das seções 11 e 12 apagou as linhas de **uma coluna** de **uma tabela**. Esta seção
# é a lista do que ficou para trás, com a prova de cada item que dá para provar localmente.
#
# **Por que importa** — Pedido de eliminação é atendido quando o dado sai de **todos** os lugares, não quando o
# `SELECT` mais óbvio volta vazio.
#
# **Como funciona — quatro lugares onde o titular ainda está**
#
# **(1) No `_delta_log`.** Cada arquivo adicionado é registrado no log com **estatísticas** por coluna —
# mínimo, máximo, nulos — usadas para *data skipping* (pular arquivo que não pode conter o valor filtrado). Se o
# login do titular for o menor ou o maior de um arquivo, ele está **escrito no JSON do log**, e o `VACUUM` não
# mexe no log.

# %%
def logins_nas_estatisticas(caminho: str) -> set[str]:
    achados = set()
    for f in sorted((Path(caminho) / "_delta_log").glob("*.json")):
        for linha in f.read_text().splitlines():
            add = json.loads(linha).get("add")
            if add and add.get("stats"):
                st = json.loads(add["stats"])
                achados |= {st.get("minValues", {}).get("actor_login"), st.get("maxValues", {}).get("actor_login")}
    return achados - {None}


for nome, caminho in (("§11 (DELETE + VACUUM)", COW), ("§12 (DELETE + REORG + VACUUM)", DV)):
    stats = logins_nas_estatisticas(caminho)
    print(f"{nome:<30} titular em {g.arquivos_log_com_valor(caminho, TITULAR)} arquivo(s) do _delta_log | "
          f"aparece como min/max de actor_login: {TITULAR in stats} | parquet: {g.contar_no_parquet_bruto(spark, caminho, 'actor_login', TITULAR)} linhas")

# %% [markdown]
# As duas tabelas foram "limpas" e o login continua legível em texto puro no log da versão 0 — o login fictício
# começa com `zzzz` justamente para cair no **máximo** das estatísticas e tornar isso visível. Com dado real, o
# mesmo acontece com quem tiver o login no extremo alfabético de um arquivo (e com a coluna inteira, quando o
# arquivo tem uma linha só).
#
# O log antigo só sai quando passa de `delta.logRetentionDuration` (30 dias por padrão) e um checkpoint novo é
# escrito. A defesa é **não coletar estatística de coluna pessoal** — de quebra, é a coluna em que *data
# skipping* por min/max menos ajuda (login e hash têm alta cardinalidade e não vêm ordenados):

# %%
SEM = str(DEMO / "sem_stats_pii")
(
    ev.select(*COLS).repartition(4).write.format("delta")
    .option("delta.dataSkippingStatsColumns", "id,type,created_at")
    .save(SEM)
)
print("tabela com delta.dataSkippingStatsColumns = 'id,type,created_at'")
print(f"  linhas do titular na tabela          : {spark.read.format('delta').load(SEM).where(do_titular).count()}")
print(f"  arquivos do _delta_log com o titular : {g.arquivos_log_com_valor(SEM, TITULAR)}")
commit0 = (Path(SEM) / "_delta_log" / "00000000000000000000.json").read_text().splitlines()
primeiro_add = next(json.loads(linha)["add"] for linha in commit0 if '"add"' in linha)
print(f"  colunas com estatística no log       : {sorted(json.loads(primeiro_add['stats'])['minValues'])}")

# %% [markdown]
# Mesma tabela, mesmo titular dentro, **zero** ocorrências no log. (Pelo mesmo motivo: nunca particione por
# coluna pessoal — o valor de partição vira **nome de pasta** e campo do log.)
#
# **(2) Em outras colunas da mesma tabela.** O `DELETE` do §11 filtrou `actor_login`. O Setup mostrou que o
# titular também é **dona de repositórios** em que outras contas agiram, e aparece dentro de `payload`:

# %%
cow = spark.read.format("delta").load(COW)
print(f"tabela do §11, depois de DELETE + VACUUM, linhas com actor_login = titular : {cow.where(do_titular).count()}")
print(f"  … com o login do titular em repo_name                                   : {cow.where(F.col('repo_name').startswith(TITULAR + '/')).count()}")
print(f"tabela de eventos completa: linhas de OUTROS atores com o login no payload : {ev.where(~do_titular & F.col('payload').contains(TITULAR)).count()}")
cow.where(F.col("repo_name").startswith(TITULAR + "/")).groupBy("type", g.mascara_parcial("actor_login", 5).alias("ator (mascarado)")).count().orderBy(F.desc("count")).show(3, truncate=False)

# %% [markdown]
# O esquecimento pela coluna óbvia deixou 4 linhas com o login em `repo_name`: eventos de **outro** ator (um
# bot comentando em issues do repositório dela). E na tabela completa há 4 eventos de outros atores com o login
# dentro do `payload` — texto livre, que nenhum filtro por coluna alcança. São poucos aqui; a conta cresce com
# a popularidade do titular. Decidir o que fazer com essas linhas (apagar? reescrever só o campo?) é do DPO; **saber que
# elas existem** é do engenheiro. O predicado de eliminação sai do inventário de colunas (§4), não do nome da
# coluna mais evidente.
#
# **(3) Nas outras camadas.** O dado da bronze foi copiado para a silver, agregado na gold, e talvez exportado.
#
# | Onde | O que fazer | Armadilha |
# |---|---|---|
# | **Landing** (arquivos `.json.gz`) | o arquivo bruto contém o titular: reescrever sem as linhas, ou reter por prazo curto e apagar o arquivo inteiro | reprocessar a bronze a partir do landing **ressuscita** o titular |
# | **Bronze** | `DELETE` (+ `REORG`) + `VACUUM` | é fonte de stream: ver abaixo |
# | **Silver** | idem, pela chave do titular (login ou pseudônimo) | SCD2 (nb 05) guarda **versões antigas** da pessoa — todas saem |
# | **Gold** | agregados sem identificador: em geral nada a fazer | agregado com grupo de 1 pessoa (k = 1, §6) ainda identifica |
# | **Stream lendo a tabela** | `DELETE` na origem quebra o stream de append | `skipChangeCommits` ignora o commit — e **não propaga** a exclusão: o destino precisa do seu próprio `DELETE` |
# | **Checkpoints / state store** | estado de agregação com chave pessoal | expira pelo watermark, não por `DELETE` |
# | **Cofre de tokens, features, índices vetoriais, cache de BI** | cada um tem o seu mecanismo | são os que ninguém lembra |
# | **Cópias em dev, extrações, planilhas** | não ter | lineage não vê (§9) |
#
# A lista de tabelas sai do **lineage** (§9) e da classificação (§4). E a ordem importa: **da origem para o
# destino**, senão um reprocessamento traz o dado de volta. Uma tabela de controle de pedidos (titular, data,
# tabelas tratadas, versão do `VACUUM`) é a evidência de cumprimento — e ela guarda o **pseudônimo**, não o login.
#
# **(4) Em backups e na lixeira do storage ☁️.** Depois do `VACUUM`:
#
# - **Soft delete do ADLS** (blob e contêiner): arquivo "apagado" fica recuperável pelo período configurado.
#   **Versionamento** e **snapshots** de blob guardam cópias anteriores.
# - **Backup** (cópia para outra conta, `DEEP CLONE` para DR, replicação geográfica): cópias inteiras, com o titular.
# - **`UNDROP`** de tabela gerenciada: 7 dias (§2).
#
# Ninguém reescreve fita de backup por titular. A prática aceita é: backup com **prazo de expiração** curto e
# documentado, e um procedimento que **reaplica os pedidos de eliminação** depois de qualquer restauração (a
# tabela de controle acima é o que permite isso). O prazo total que um dado "apagado" sobrevive é a **soma**:
# retenção do `VACUUM` + soft delete do storage + ciclo de vida do backup.
#
# ### Crypto-shredding: apagar a chave em vez do dado 🧪
#
# **O que é** — Cada titular tem **a sua** chave de criptografia; o dado pessoal é gravado cifrado com ela.
# "Eliminar" passa a ser **destruir a chave**: todas as cópias do dado — em toda camada, em todo backup — viram
# bytes ilegíveis de uma vez (*crypto-shredding*, "picotar criptograficamente").

# %%
CHAVES, CRIPTO = str(DEMO / "cofre_chaves"), str(DEMO / "eventos_cripto")
pseudo = ev.select("id", "type", "actor_login", g.hmac_sha256("actor_login", chave).alias("actor_pseudo"))

# 1) uma chave AES-256 por titular, indexada pelo pseudônimo (o cofre não guarda o login) — gravada antes de usar
g.chaves_por_titular(pseudo, "actor_pseudo").write.format("delta").save(CHAVES)
chaves = spark.read.format("delta").load(CHAVES).withColumnRenamed("titular", "actor_pseudo")

# 2) a tabela de eventos guarda o pseudônimo (para juntar) e o login CIFRADO com a chave do titular
(
    pseudo.join(chaves, "actor_pseudo")
    .select("id", "type", "actor_pseudo", g.criptografar("actor_login", "chave_hex").alias("actor_login_cifrado"))
    .write.format("delta")
    .save(CRIPTO)
)


def abrir():
    cripto = spark.read.format("delta").load(CRIPTO)
    chaves_agora = spark.read.format("delta").load(CHAVES).withColumnRenamed("titular", "actor_pseudo")
    return cripto.join(chaves_agora, "actor_pseudo", "left").select(
        "id", "actor_pseudo", g.descriptografar("actor_login_cifrado", "chave_hex").alias("actor_login_aberto")
    )


PSEUDO_TITULAR = g.hmac_sha256_py(TITULAR, chave)
eh_dela = F.col("actor_pseudo") == PSEUDO_TITULAR


def situacao(rotulo: str) -> None:
    a = abrir()
    r = a.agg(
        F.sum(eh_dela.cast("int")).alias("linhas"),
        F.sum((eh_dela & F.col("actor_login_aberto").isNotNull()).cast("int")).alias("legiveis"),
        F.sum((~eh_dela & F.col("actor_login_aberto").isNotNull()).cast("int")).alias("outros_legiveis"),
        F.sum((~eh_dela).cast("int")).alias("outros"),
    ).first()
    print(f"{rotulo:<28} titular: {r.linhas} linhas cifradas na tabela, {r.legiveis} legíveis | demais titulares: {r.outros_legiveis:,} de {r.outros:,} legíveis")


situacao("antes")

# 3) eliminar = apagar UMA linha do cofre de chaves (e limpar o cofre, que é uma tabela minúscula)
spark.sql(f"DELETE FROM delta.`{CHAVES}` WHERE titular = '{PSEUDO_TITULAR}'")
g.vacuum_imediato(spark, CHAVES)
situacao("depois de apagar a chave")
versao_cripto = spark.sql(f"DESCRIBE HISTORY delta.`{CRIPTO}` LIMIT 1").first()["version"]
print(f"chave do titular nos parquet do cofre: {g.contar_no_parquet_bruto(spark, CHAVES, 'titular', PSEUDO_TITULAR)} | "
      f"tabela de eventos: continua na versão {versao_cripto}, nenhum arquivo reescrito")

# %% [markdown]
# A tabela de eventos **não foi tocada** (continua na versão 0, mesmas linhas, mesmos arquivos) e as linhas do
# titular ficaram ilegíveis; os outros titulares continuam abrindo. O único `DELETE` + `VACUUM` foi numa tabela
# pequena — e vale também para o backup da tabela de eventos de três meses atrás.
#
# Limites que precisam ser ditos junto:
#
# - **O pseudônimo ficou.** As linhas ainda estão ligadas entre si por `actor_pseudo`, e quem tem a chave do
#   HMAC recalcula `HMAC(login)` e reencontra as linhas do titular. Se o pedido exige romper essa ligação, o
#   pseudônimo também precisa depender de segredo por titular — ou as linhas saem de fato.
# - **O backup do cofre de chaves** precisa da mesma disciplina: chave recuperável = dado recuperável.
# - **Só cobre o que foi cifrado.** O login em `repo_name` e no `payload` (item 2) continua em claro.
# - **A leitura fica mais cara**: toda consulta que abre o valor faz `JOIN` com o cofre. Em produção o cofre é
#   um serviço de chaves (Key Vault, HSM), não uma tabela Delta, e a destruição da chave é auditada.
# - Se "tornar ilegível" equivale juridicamente a "eliminar" é entendimento a validar com o DPO.
#
# > 🎤 **Resposta de 30 s:** "DELETE mais VACUUM resolve uma tabela. Um pedido de eliminação de verdade passa por
# > todos os lugares: o landing, senão um reprocessamento ressuscita o dado; bronze, silver e as versões de SCD;
# > outras colunas e texto livre onde o identificador aparece; as estatísticas min/max no delta log, que o
# > VACUUM não limpa; a lixeira do storage e os backups. Eu parto do lineage para listar as tabelas, executo da
# > origem para o destino e registro o que foi feito. Onde reescrever tudo é inviável, a alternativa é
# > crypto-shredding: uma chave por titular, e eliminar vira destruir a chave."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Checkpoint do log**: a cada N commits o Delta grava um checkpoint parquet dentro do `_delta_log` com o
#   estado consolidado — as estatísticas dos arquivos **ativos** vão junto. Entradas de arquivos removidos somem
#   do checkpoint; os JSON antigos só somem na limpeza do log.
# - **Stream + exclusão, a forma certa**: Change Data Feed na origem e um `MERGE` no destino que aplica os
#   `delete` — ou um job de eliminação que percorre a lista de tabelas. `skipChangeCommits` sozinho só evita o erro.
# - **Idempotência**: o job de eliminação roda de novo depois de restauração, de reprocessamento e de backfill.
#   Mantenha uma **lista de supressão** (pseudônimos eliminados) consultada na ingestão, para o titular não
#   voltar no próximo arquivo do landing.
# - **A lista de supressão é dado pessoal?** É pseudonimizada e tem finalidade própria (cumprir o pedido). É o
#   argumento usual para mantê-la — outra decisão para registrar com o DPO.
# - **Agregados**: se a gold tem `COUNT(DISTINCT actor)` por dia, apagar o titular muda o número histórico.
#   Recalcular ou não é decisão de negócio; anonimizado de verdade (art. 12) pode ficar.
# - **Modelos de ML treinados com o dado**: não há `DELETE` para peso de modelo. Registro de quais dados
#   treinaram qual versão (lineage de modelo no UC) é o mínimo para responder.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Crypto-shredding exige desenhar o esquema para isso **desde o início** (coluna cifrada, chave por titular,
#   pseudônimo para junção). Adaptar depois é reescrever tudo — o custo que se queria evitar.
# - Desligar estatística de coluna pessoal tira *data skipping* dela: busca por titular vira varredura, a menos
#   que a tabela seja clusterizada por um *bucket* do pseudônimo (nb 09).

# %% [markdown]
# ## 14. Papel do engenheiro de dados × DPO/encarregado
#
# **O que é** — O **encarregado** (DPO, *data protection officer* — LGPD art. 41) é o canal entre a empresa, os
# titulares e a ANPD (Autoridade Nacional de Proteção de Dados): recebe os pedidos, orienta a empresa, responde
# à autoridade. Ele decide **o quê** e **por quê**. O engenheiro de dados entrega o **como** — e a prova.
#
# **Por que importa** — Entrevista de sênior testa se você sabe **onde termina a sua decisão**. Engenheiro que
# "decide" a base legal ou o prazo de retenção sozinho é risco; engenheiro que espera o jurídico dizer que
# `DELETE` não apaga também.
#
# **Como funciona**
#
# | Assunto | DPO / jurídico / dono do dado decide | Engenheiro de dados entrega |
# |---|---|---|
# | Classificação | os critérios e os níveis | a política em código, aplicada como metadado e tag (§4), com teste |
# | Base legal e finalidade | qual é, por tabela | o registro disso junto da tabela; impedir uso fora da finalidade (grants, §3) |
# | Retenção | o prazo | o job de `DELETE` + `VACUUM`, o alerta se ele falhar, o prazo real somado (§13) |
# | Pedido do titular | se procede, o escopo, a resposta | achar o titular em todas as camadas, executar, registrar evidência (§11–§13) |
# | Quem acessa o quê | os papéis e as exceções | grupos, grants, máscara, filtro — versionados (§3, §8) |
# | Incidente | comunicar ANPD e titulares | dizer quais dados e quais titulares, com base em auditoria (§9) |
# | Técnica de proteção | o nível de risco aceitável | a **explicação honesta** do que cada técnica garante (hash ≠ anonimização, §5) |
#
# A última linha é a contribuição mais valiosa do engenheiro: **traduzir**. O DPO não tem como saber que o time
# travel devolve o dado apagado, que o deletion vector mantém o parquet, que o `repo_name` desfaz a máscara do
# login. Tudo o que este notebook provou é o tipo de coisa que precisa chegar a ele antes de virar incidente.
#
# > 🎤 **Resposta de 30 s:** "O DPO decide o quê e por quê: base legal, prazo, se o pedido do titular procede. Eu
# > entrego o como e a evidência: política de classificação em código, pipeline que respeita retenção, rotina de
# > eliminação que cobre todas as camadas, e auditoria para demonstrar. E tenho o dever de avisar o que a
# > técnica não garante — que hash não é anonimização, que DELETE não apaga do disco. Eu não escolho a base
# > legal; eu garanto que a decisão dele seja verdade no dado."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Outros papéis na mesa** (o time multidisciplinar): **segurança da informação** (rede,
#   identidade, cofre, resposta a incidente), **dono do dado** (*data owner* — a área de negócio que responde
#   pelo conteúdo e aprova acesso), ***data steward*** (cuida de qualidade e metadado no dia a dia), plataforma
#   (workspaces, UC, Terraform).
# - **Como isso entra na rotina ágil**: privacidade como critério de aceite ("coluna nova tem classificação",
#   "tabela nova tem prazo de retenção"), revisão de política em *pull request*, e o DPO como parte interessada
#   no refinamento de histórias que criam ou cruzam dado pessoal — não como aprovador no fim.
# - **O que automatizar**: teste que falha se uma tabela não tem classificação; teste que falha se coluna
#   pessoal aparece na gold; alerta se o job de `VACUUM`/retenção não rodou; relatório periódico de grants.
# - **Quando escalar**: pedido de acesso fora do papel, uso novo para dado antigo, cópia para fora do ambiente,
#   fornecedor novo (inclusive LLM — nb 12) recebendo dado.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - Engenheiro bloqueando tudo "por causa da LGPD" sem consultar o DPO é tão errado quanto ignorar a lei: muito
#   tratamento é legítimo e a decisão não é dele.
# - Automatizar a **execução** de um pedido de eliminação é bom; automatizar a **decisão** de que o pedido
#   procede, não — há hipóteses legais de conservação (art. 16).

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# O que muda quando este notebook sai do laptop:
#
# | Aqui (🧪 local) | No Databricks / Azure (☁️) |
# |---|---|
# | `spark_catalog`, dois níveis, sem permissão | Unity Catalog: `catalogo.schema.objeto`, grants, ownership (§1–§3) |
# | Comentário de coluna e `TBLPROPERTIES` | tags; **tags governadas** definidas na conta (§4) |
# | `OSSLH_PSEUDO_KEY` em variável de ambiente | Azure Key Vault + secret scope; `dbutils.secrets.get` (§5) |
# | View com `DECLARE VARIABLE papel` | `is_account_group_member()`, column mask, row filter, política ABAC (§8) |
# | `DESCRIBE HISTORY` (só escritas, sem usuário) | `system.access.audit`, `table_lineage`, `column_lineage` (§9) |
# | `VACUUM` manual; deletion vectors desligados por padrão | *predictive optimization* faz `VACUUM` em tabela gerenciada; **DVs ligados por padrão** → `REORG … APPLY (PURGE)` faz parte do fluxo (§12) |
# | Arquivo apagado some do disco | soft delete, versionamento e snapshots do ADLS; `UNDROP` de 7 dias (§13) |
#
# O pipeline no Databricks, com a pseudonimização materializada e a governança declarada:
#
# ```python
# # ☁️ job da silver — roda como service principal (sp-pipeline-silver), com a chave vinda do Key Vault
# from oss_lakehouse.governance import hmac_sha256
#
# chave = dbutils.secrets.get("kv-lakehouse", "pseudo-hmac-key").encode()
# (spark.read.table("prod.bronze.gh_events")
#       .withColumn("actor_pseudo_id", hmac_sha256("actor.login", chave))
#       .drop("payload")                       # minimização: a silver analítica não leva texto livre
#       .write.mode("append").saveAsTable("prod.silver.gh_events"))
# ```
#
# ```sql
# -- ☁️ governança declarada uma vez (em Terraform ou num job de setup), revisada em PR
# ALTER TABLE prod.silver.gh_events ALTER COLUMN actor_login SET TAGS ('pii' = 'login');
# ALTER TABLE prod.silver.gh_events SET TBLPROPERTIES (
#   'delta.deletedFileRetentionDuration' = 'interval 7 days',    -- teto de sobrevida do dado apagado
#   'delta.dataSkippingStatsColumns'     = 'id,type,created_at,event_date'   -- sem min/max de coluna pessoal
# );
#
# -- ☁️ rotina de eliminação (por lote de pedidos), da origem para o destino
# DELETE FROM prod.bronze.gh_events WHERE actor.login IN (SELECT login FROM prod.governanca.pedidos_pendentes);
# DELETE FROM prod.silver.gh_events WHERE actor_pseudo_id IN (SELECT pseudo_id FROM prod.governanca.pedidos_pendentes);
# REORG TABLE prod.bronze.gh_events APPLY (PURGE);
# REORG TABLE prod.silver.gh_events APPLY (PURGE);
# -- o VACUUM roda depois da retenção (predictive optimization, ou job agendado em tabela externa)
# VACUUM prod.bronze.gh_events;
# VACUUM prod.silver.gh_events;
# ```
#
# Pontos de Azure que a entrevista costuma tocar:
#
# - **Identidade**: usuários e grupos vêm do **Microsoft Entra ID** (SCIM ou sincronização automática); job roda
#   como **service principal**; o UC acessa o ADLS pela **identidade gerenciada** do Access Connector — nenhuma
#   chave de storage em código (nb 14).
# - **Key Vault**: *secret scope* com backend no Key Vault; a chave do HMAC com rotação planejada e *purge
#   protection* ligada (uma chave apagada por engano é uma base inteira sem junção).
# - **Storage**: soft delete e versionamento do ADLS entram na conta do prazo de eliminação; *lifecycle
#   management* expira o landing; firewall + private endpoint restringem quem alcança os arquivos por fora do UC.
# - **Região**: `brazilsouth` para workspace, storage e metastore mantém o dado no país (transferência
#   internacional é assunto do art. 33).
# - **Microsoft Purview**: catálogo corporativo que pode varrer o UC — a classificação daqui alimenta o
#   inventário da empresa, que vai além do Databricks.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Explique a hierarquia do Unity Catalog e o que é o namespace de três níveis.**
# <details><summary>Resposta</summary>
# Metastore (um por região, no nível da conta) → catalog → schema → objeto (table, view, volume, function,
# model). Todo objeto é referenciado como <code>catalogo.schema.objeto</code>. Storage credential e external
# location ficam direto no metastore. Permissão, tag, lineage e auditoria são centrais e valem em todos os
# workspaces ligados ao metastore. Local não existe: o <code>spark_catalog</code> tem dois níveis e nem aceita
# <code>GRANT</code> (§1).
# </details>
#
# **2. Managed × external table: qual a diferença prática e qual você escolhe?**
# <details><summary>Resposta</summary>
# Managed: o catálogo é dono dos arquivos; <code>DROP</code> apaga o dado (no UC, depois de 7 dias de
# <code>UNDROP</code>) e a plataforma faz a manutenção. External: eu informo o <code>LOCATION</code>;
# <code>DROP</code> só tira o registro e os arquivos ficam — provado no §2. Padrão managed; external quando outro
# sistema usa o mesmo caminho. Para LGPD, "dropei a tabela externa" não eliminou nada.
# </details>
#
# **3. Um analista tem <code>SELECT</code> na tabela e mesmo assim recebe erro de permissão. Por quê?**
# <details><summary>Resposta</summary>
# Falta <code>USE CATALOG</code> no catálogo ou <code>USE SCHEMA</code> no schema: são pré-requisitos para
# "atravessar" a hierarquia. Outras causas: o catálogo está com <em>workspace binding</em> e ele está em outro
# workspace; o compute não tem modo de acesso do UC; ou há uma política ABAC em conflito (duas máscaras na mesma
# coluna fazem a consulta falhar).
# </details>
#
# **4. Por que "fazemos hash do CPF" não é anonimização? O que você faria?**
# <details><summary>Resposta</summary>
# Hash sem segredo é determinístico e o espaço de CPFs é pequeno e enumerável: o atacante calcula o hash de todos
# os candidatos e faz um join (ataque de dicionário). No §5, com um dicionário de duas horas de dado público,
# 60,2% dos eventos foram reidentificados em segundos. Eu usaria HMAC-SHA256 com chave no Key Vault para o
# pseudônimo de junção — e chamaria de pseudonimização: quem tem a chave reverte, então segue sendo dado pessoal
# (art. 13 §4º). Sal conhecido não resolve; sal aleatório por linha quebra a junção.
# </details>
#
# **5. Quando usar HMAC, tokenização, criptografia ou mascaramento?**
# <details><summary>Resposta</summary>
# HMAC: ninguém precisa reverter, mas preciso juntar e contar. Tokenização: um grupo restrito precisa reverter,
# com auditoria — o cofre fica separado e precisa ser persistido (token aleatório não é recalculável).
# Criptografia: preciso guardar o valor; não serve para join porque cada cifra é diferente (§6). Máscara: só
# exibição parcial. Generalização/k-anonimato: publicar ou reter sem identificar, medindo o k.
# </details>
#
# **6. Diferença entre dynamic view, row filter/column mask e ABAC. Qual é o recomendado hoje?**
# <details><summary>Resposta</summary>
# Dynamic view: <code>CASE WHEN is_account_group_member(...)</code> no SQL da view; o usuário consulta outro
# objeto. Row filter/column mask: função SQL presa à tabela por <code>ALTER TABLE</code>; uma a uma. ABAC:
# <code>CREATE POLICY</code> no catálogo ou schema, casando por tag governada — cobre tabelas futuras e o dono
# da tabela não remove. Para regra corporativa a documentação recomenda ABAC (exige serverless ou DBR 16.4+).
# A identidade do pipeline vai no <code>EXCEPT</code>, senão a tabela derivada é gravada mascarada.
# </details>
#
# **7. Login do GitHub é público. Preciso me preocupar com LGPD?**
# <details><summary>Resposta</summary>
# Sim. É dado pessoal porque identifica uma pessoa natural; "público" descreve o acesso. O art. 7º §4º dispensa
# o consentimento para dado tornado manifestamente público pelo titular, mas mantém os direitos do titular e os
# princípios — finalidade, necessidade, segurança. Posso analisar atividade pública; não posso montar perfil
# para outra finalidade, guardar para sempre nem ignorar pedido de eliminação.
# </details>
#
# **8. Um titular pediu eliminação. Você rodou <code>DELETE</code>. Acabou?**
# <details><summary>Resposta</summary>
# Não. O <code>DELETE</code> é lógico: os arquivos antigos ficam e o time travel devolve o dado (§11). Falta
# <code>VACUUM</code> depois da retenção; se a tabela tem deletion vectors (padrão no Databricks),
# <code>REORG TABLE … APPLY (PURGE)</code> antes, senão o parquet com o dado continua na versão atual (§12).
# E falta o resto: outras colunas e texto livre, landing, silver e versões de SCD, estatísticas no
# <code>_delta_log</code>, soft delete do storage e backups (§13).
# </details>
#
# **9. Por que o <code>VACUUM</code> tem retenção mínima de 7 dias e o que acontece se eu forçar zero?**
# <details><summary>Resposta</summary>
# Para proteger leitores longos, streams atrasados e, principalmente, escritas concorrentes: arquivo de transação
# ainda não commitada não está no log e seria apagado, corrompendo a tabela. Forçar zero exige desligar
# <code>retentionDurationCheck</code>; só em tabela sem concorrência, como demonstração. Efeito colateral
# sempre: time travel para antes do <code>VACUUM</code> deixa de funcionar.
# </details>
#
# **10. O que são deletion vectors e como afetam a LGPD?**
# <details><summary>Resposta</summary>
# Arquivos <code>.bin</code> que marcam linhas apagadas, evitando reescrever o parquet (merge-on-read). No §12
# o <code>DELETE</code> gravou zero arquivos de dados e, depois do <code>VACUUM</code> com retenção zero, o
# parquet ainda tinha todas as linhas do titular — porque o arquivo continua na versão atual. Só
# <code>REORG … APPLY (PURGE)</code> + <code>VACUUM</code> remove.
# </details>
#
# **11. Depois de DELETE, REORG e VACUUM, onde o dado ainda pode estar?**
# <details><summary>Resposta</summary>
# Nas estatísticas min/max do <code>_delta_log</code> (provado no §13; evita-se com
# <code>delta.dataSkippingStatsColumns</code>); em valores de partição; em outras colunas e em texto livre; no
# landing; em tabelas derivadas e em versões de SCD2; no state store e em cofres de token; em cache do cluster;
# no soft delete/versionamento do ADLS; em backups e clones; em cópias de dev e exportações.
# </details>
#
# **12. O que é crypto-shredding e quando vale a pena?**
# <details><summary>Resposta</summary>
# Cifrar o dado pessoal com uma chave por titular e, no pedido de eliminação, destruir a chave: todas as cópias,
# inclusive backups, ficam ilegíveis sem reescrever nada (§13: a tabela de eventos ficou na versão 0). Vale
# quando reescrever tudo é inviável (backups imutáveis, muitas cópias). Custos: tem de ser desenhado desde o
# início, toda leitura do valor faz join com o cofre, o backup das chaves precisa da mesma disciplina, e o
# pseudônimo determinístico que ficar ainda liga as linhas.
# </details>

# %% [markdown]
# ## Resumo
#
# - **Unity Catalog**: metastore → catalog → schema → objeto, nome de três níveis; grants aditivos que herdam
#   para baixo, dados a **grupos** do Entra ID; `USE CATALOG`/`USE SCHEMA` são a porta. Managed apaga no `DROP`,
#   external não.
# - **Hash sem sal não protege**: ataque de dicionário é um `JOIN`. Pseudônimo = **HMAC com chave no Key
#   Vault**; continua dado pessoal. Tokenização e criptografia quando alguém precisa reverter; k-anonimato se
#   mede.
# - **Máscara e filtro**: dynamic view → row filter/column mask → **ABAC por tag governada**. Mascarar uma
#   coluna não protege o valor que aparece em outra (`repo_name`, `payload`).
# - **Eliminação em Delta**: `DELETE` → (`REORG … APPLY (PURGE)` se houver deletion vectors) → `VACUUM` depois
#   da retenção. E depois: log, outras colunas, landing, derivadas, backups. Alternativa: crypto-shredding.
# - **LGPD**: público ≠ não pessoal; pseudonimizado ≠ anonimizado; o DPO decide o quê, o engenheiro garante o
#   como — e avisa o que a técnica não garante.

# %%
spark.stop()
