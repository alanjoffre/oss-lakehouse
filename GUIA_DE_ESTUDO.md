# Guia de estudo

> Gerado por `scripts/build_guia.py` a partir da seção **Perguntas de entrevista** de cada notebook. Não editar à mão: mude a pergunta no notebook e rode `make guia`.

**199 perguntas** em 18 notebooks. Clique na pergunta para abrir a resposta curta; o notebook indicado tem a demonstração rodando e o aprofundamento.

## Índice por tema

| Notebook | Perguntas |
|---|---|
| [00 · Mapa de competências e arquitetura](#00-mapa-de-competencias-e-arquitetura) | 11 |
| [01 · Ambiente: Spark local, Databricks e Azure](#01-ambiente-local-databricks-azure) | 11 |
| [02 · Python avançado para engenharia de dados](#02-python-avancado) | 12 |
| [03 · Ingestão incremental de arquivos: landing → bronze, checkpoint e Auto Loader](#03-ingestao-arquivos-auto-loader) | 9 |
| [04 · Ingestão de API REST incremental (GitHub)](#04-ingestao-api-incremental) | 10 |
| [05 · Bronze → Silver: tipagem, deduplicação, MERGE e SCD tipo 2](#05-bronze-silver-merge-scd2) | 10 |
| [06 · Streaming com Structured Streaming (Wikimedia EventStreams)](#06-streaming-structured-streaming) | 11 |
| [07 · Gold: modelagem dimensional (Kimball), star schema e Liquid Clustering](#07-gold-modelagem-dimensional) | 10 |
| [08 · Qualidade de dados e contratos: expectations, quarentena, freshness, volume e pipelines declarativos](#08-qualidade-e-contratos) | 10 |
| [09 · Performance no Spark](#09-performance-spark) | 12 |
| [10 · Delta Lake por dentro](#10-delta-lake-por-dentro) | 11 |
| [11 · Governança, Unity Catalog e LGPD](#11-governanca-unity-catalog-lgpd) | 12 |
| [12 · IA aplicada à engenharia de dados](#12-ia-aplicada-engenharia-de-dados) | 12 |
| [13 · Git, CI/CD e Declarative Automation Bundles](#13-cicd-git-asset-bundles) | 12 |
| [14 · Azure com Terraform + Azurite (custo zero)](#14-azure-terraform-azurite) | 10 |
| [15 · Observabilidade e custo](#15-observabilidade-e-custo) | 12 |
| [16 · Live coding: SQL e PySpark](#16-live-coding-sql-pyspark) | 12 |
| [17 · Simulado de system design e troubleshooting](#17-simulado-system-design) | 12 |

<a id="00-mapa-de-competencias-e-arquitetura"></a>

## 00 · Mapa de competências e arquitetura

Notebook: [`00_mapa_de_competencias_e_arquitetura.ipynb`](notebooks/00_mapa_de_competencias_e_arquitetura.ipynb)

<details>
<summary><b>1. O que é um lakehouse e em que difere de um data warehouse?</b></summary>

Tabelas transacionais (log tipo Delta/Iceberg) sobre arquivos abertos em object storage: ACID, MERGE e time
travel com storage barato e acesso por vários motores. O warehouse tem formato fechado e acoplado ao motor;
serve SQL muito bem, mas ML e semiestruturado custam mais e costumam exigir uma segunda cópia.

</details>

<details>
<summary><b>2. O que entra em cada camada do Medallion?</b></summary>

Bronze: bruto, append-only, com linhagem. Silver: tipado, deduplicado, com chave e histórico, conformado.
Gold: modelo de consumo (fatos/dimensões, agregados). Quarentena para o reprovado.

</details>

<details>
<summary><b>3. Quais as críticas ao Medallion?</b></summary>

Não é modelagem; triplica storage/compute; nomes vagos sem contrato; latência por salto; em escala, domínio
importa mais que camada. Contrato por tabela resolve mais que a cor da camada.

</details>

<details>
<summary><b>4. Por que guardar o payload bruto na bronze em vez de inferir o schema?</b></summary>

O formato muda por tipo e ao longo do tempo; inferência quebra ou cria colunas esparsas. Bruto + schema
explícito na silver = bronze estável e reprocessável (ADR 0003). No Databricks, VARIANT é a evolução.

</details>

<details>
<summary><b>5. Batch, micro-batch ou streaming: como você decide?</b></summary>

Pela latência exigida e pelo custo. Analytics de hora em hora: Structured Streaming com `availableNow` agendado.
Segundos: micro-batch contínuo. Milissegundos: motor de streaming de baixa latência (real-time mode, Flink).

</details>

<details>
<summary><b>6. Lambda ou Kappa?</b></summary>

Kappa quando há log reprocessável — um código só. Lambda duplica regra em dois motores. Com Delta, a tabela é o
log, então o lakehouse tende naturalmente para Kappa.

</details>

<details>
<summary><b>7. ETL ou ELT?</b></summary>

ELT por padrão (bruto preservado, transformação escalável no destino); "T" antes do "L" para PII e redução de
volume — EtLT.

</details>

<details>
<summary><b>8. Delta, Iceberg ou Hudi?</b></summary>

Delta no Databricks (nativo, Photon, Liquid, Predictive Optimization); Iceberg quando vários motores de
fornecedores diferentes escrevem a mesma tabela; Hudi para upsert/CDC intenso com índice. UniForm reduz o
custo da escolha.

</details>

<details>
<summary><b>9. Lakeflow Jobs, ADF ou Airflow?</b></summary>

Jobs quando tudo roda no Databricks (zero infra extra, bundle, system tables). ADF para cópia de fontes
on-premises/SaaS. Airflow quando se orquestra muitos sistemas heterogêneos e se aceita operar o Airflow.

</details>

<details>
<summary><b>10. O que é um ADR e o que vai nele?</b></summary>

Registro curto de uma decisão de arquitetura: contexto, decisão, alternativas, consequências (incluindo as
ruins). Imutável; mudança vira ADR novo que substitui o anterior.

</details>

<details>
<summary><b>11. Como você levaria este pipeline do laptop para a Azure?</b></summary>

Terraform para workspace, ADLS, Access Connector e Key Vault; Unity Catalog com external locations; o mesmo
wheel executado por um Lakeflow Job definido em bundle; GitHub Actions com OIDC para deploy em dev e prod;
só a configuração (`OSSLH_DATA_ROOT`) muda.

</details>

<a id="01-ambiente-local-databricks-azure"></a>

## 01 · Ambiente: Spark local, Databricks e Azure

Notebook: [`01_ambiente_local_databricks_azure.ipynb`](notebooks/01_ambiente_local_databricks_azure.ipynb)

<details>
<summary><b>1. Qual a diferença entre driver e executor? Onde roda uma UDF Python?</b></summary>

Driver roda o programa, planeja e agenda; executors executam tasks sobre partições. A UDF roda nos executors,
num processo Python ao lado da JVM, com serialização (Arrow, no caso de pandas UDF) — por isso é cara.

</details>

<details>
<summary><b>2. O que significa `local[4]`? E `local[*]`?</b></summary>

Modo local: driver e executor na mesma JVM com 4 threads (ou todos os núcleos com `*`). Útil para desenvolvimento
e testes; não representa custo de rede de um cluster.

</details>

<details>
<summary><b>3. Aumentei `spark.driver.memory` no notebook e o OOM continua. Por quê?</b></summary>

É config estática da JVM: só vale na criação do processo. Precisa ir na config do cluster/`spark-submit`/builder
antes de a sessão subir. `spark.conf.set` recusa com erro; o builder com `getOrCreate()` aceita calado e
`spark.conf.get` passa a devolver o valor novo — mas o heap real não muda (prova na §2).

</details>

<details>
<summary><b>4. Por que a versão do Delta precisa casar com a do Spark?</b></summary>

O JAR do Delta usa APIs internas do Spark; é publicado por versão (`delta-spark_4.2_2.13`). Descasar dá
`NoSuchMethodError`/`ClassNotFoundException` em tempo de execução. No Databricks, o runtime já traz o Delta certo.

</details>

<details>
<summary><b>5. All-purpose, job compute, serverless e SQL warehouse: quando usar cada um?</b></summary>

All-purpose para trabalho interativo; job compute (ou serverless) para jobs agendados — DBU mais barata e vida
curta; serverless quando o início rápido e a gestão zero valem a perda de controle; SQL warehouse para SQL/BI.

</details>

<details>
<summary><b>6. Quais os limites da Databricks Free Edition?</b></summary>

Só serverless (com cota diária), 1 SQL warehouse 2X-Small, 5 tasks de job simultâneas, 1 pipeline ativo por tipo,
1 workspace, sem SSO/rede privada, sem R/Scala, uso não comercial (doc de 29/09/2026).

</details>

<details>
<summary><b>7. O que é Spark Connect e o que deixa de funcionar com ele?</b></summary>

Cliente-servidor por gRPC: o cliente envia o plano lógico. É a base do Databricks Connect e do serverless.
Não há `sparkContext`, RDD nem acesso à JVM.

</details>

<details>
<summary><b>8. Como você guarda o token de uma API usada pelo pipeline no Azure Databricks?</b></summary>

No Key Vault, exposto como secret scope (somente leitura, modelo de *access policy*), lido com
`dbutils.secrets.get` ou referenciado na config do cluster como `{{secrets/scope/key}}`. ACL do scope para o
service principal do job. Nunca em código, widget ou variável em texto.

</details>

<details>
<summary><b>9. O que é VNet injection e por que uma empresa exige?</b></summary>

O plano de computação usa uma VNet do cliente (subnets host e container), permitindo NSG, rota pelo firewall
corporativo, private endpoints para o storage e nós sem IP público (Secure Cluster Connectivity).

</details>

<details>
<summary><b>10. O que funciona igual local e no Databricks, e o que não?</b></summary>

Igual: API DataFrame/SQL, semântica do Delta, testes. Diferente: Auto Loader, Unity Catalog, Photon, serverless,
`dbutils`, system tables. A lacuna fecha com deploy em `dev` e teste de integração antes de prod.

</details>

<details>
<summary><b>11. Volumes × DBFS mounts?</b></summary>

Volumes são objetos do Unity Catalog para arquivos, com grants e lineage; mounts são legado sem governança
(qualquer um no workspace via o mount). Arquivo novo vai para Volume.

</details>

<a id="02-python-avancado"></a>

## 02 · Python avançado para engenharia de dados

Notebook: [`02_python_avancado.ipynb`](notebooks/02_python_avancado.ipynb)

<details>
<summary><b>1. Qual a diferença entre iterator e generator? Quando um generator economiza memória?</b></summary>

Iterator é o protocolo (`__iter__`/`__next__`); generator é uma forma de implementá-lo com `yield`. Economiza
memória quando o consumidor processa e descarta item a item — medido na §1: pico da lista inteira em centenas
de MB contra menos de 1 MB do generator. Não economiza se você fizer `list(gen)`.

</details>

<details>
<summary><b>2. Por que `itertools.groupby` "erra" a contagem?</b></summary>

Porque agrupa elementos consecutivos com a mesma chave. Sem ordenar pela chave, o mesmo valor gera vários
grupos. Para contar, `Counter`; para agrupar em streaming, a entrada precisa vir ordenada.

</details>

<details>
<summary><b>3. Escreva um decorator de retry. O que um sênior acrescenta?</b></summary>

Backoff exponencial com jitter, lista explícita de exceções re-tentáveis, teto de espera e de tentativas,
`functools.wraps`, `ParamSpec`, `sleep` injetável para teste — e a pergunta "a operação é idempotente?".

</details>

<details>
<summary><b>4. Como garantir que um consumidor nunca leia um arquivo pela metade?</b></summary>

Escrever em temporário e renomear atomicamente (`os.replace`, mesmo filesystem) dentro de um context manager
que apaga o temporário em erro. Em object storage não há rename atômico: o Delta resolve com o transaction log.

</details>

<details>
<summary><b>5. `Protocol` ou classe abstrata (ABC)?</b></summary>

`Protocol` é tipagem estrutural: qualquer classe com os métodos certos serve, sem herdar — ótimo para fakes em
teste e para código de terceiros. ABC exige herança e pode carregar implementação padrão. Em fronteiras
(sinks, clientes) prefiro Protocol.

</details>

<details>
<summary><b>6. Dataclass ou pydantic para representar um evento?</b></summary>

Na fronteira (JSON externo), pydantic: valida, converte e devolve todos os erros. No miolo, dataclass
(`frozen`, `slots`): leve e sem custo de validação. Em volume de big data, nenhum dos dois linha a linha:
schema do Spark + regras de qualidade.

</details>

<details>
<summary><b>7. Threads ou processos para baixar 500 arquivos? E para parsear 500 arquivos?</b></summary>

Baixar é espera de rede: threads (ou asyncio), com limite de concorrência. Parsear JSON em Python puro é CPU:
processos, devolvendo resultado agregado. Medido na §8: parse com threads não acelera (piorou); com processos acelera.
Em volume de verdade, Spark.

</details>

<details>
<summary><b>8. O que é o GIL e o que muda com o Python free-threaded?</b></summary>

Lock que permite uma thread executando bytecode por vez. Extensões em C que o soltam (zlib, NumPy) paralelizam
com threads. O build free-threaded (3.13 experimental, 3.14 suportado e opcional) remove o GIL, mas depende do
suporte de cada extensão e expõe race conditions; eu mediria antes de adotar.

</details>

<details>
<summary><b>9. Como você trata uma linha corrompida num arquivo de 10 milhões de linhas?</b></summary>

Isolo em quarentena com arquivo, linha e motivo, e o lote segue — com limiar de rejeição que faz o job falhar
se a fonte quebrou inteira. Erro de sistema (credencial, rede) é outra classe: falha alto com retry.

</details>

<details>
<summary><b>10. Por que sua UDF deixou o job 10× mais lento?</b></summary>

UDF Python serializa cada linha JVM↔Python e é opaca ao otimizador (sem pushdown, sem Photon). Troco por função
nativa; se não houver, pandas UDF (Arrow, lotes colunares). O `explain()` mostra `BatchEvalPython`.

</details>

<details>
<summary><b>11. pandas, Polars ou Spark para 2 GB de CSV diário?</b></summary>

Cabe numa máquina: Polars (ou pandas) resolve com menos custo e latência. Se o destino é o lakehouse
governado e o volume cresce, Spark no Databricks — talvez em serverless para não pagar cluster ocioso.

</details>

<details>
<summary><b>12. Como você empacota e entrega código Python para o Databricks?</b></summary>

Pacote com `pyproject.toml` e layout src, lock de dependências (uv), testes no CI, wheel versionada instalada no
job pelo Asset Bundle. Notebook só orquestra; nada de lógica copiada entre notebooks.

</details>

<a id="03-ingestao-arquivos-auto-loader"></a>

## 03 · Ingestão incremental de arquivos: landing → bronze, checkpoint e Auto Loader

Notebook: [`03_ingestao_arquivos_auto_loader.ipynb`](notebooks/03_ingestao_arquivos_auto_loader.ipynb)

<details>
<summary><b>1. Como você garante que um arquivo é processado exatamente uma vez?</b></summary>

Checkpoint (offsets antes, commits depois, log dos arquivos vistos) + sink idempotente (Delta registra
`txn(appId, version)` e ignora lote repetido). Mostrado abrindo os arquivos.

</details>

<details>
<summary><b>2. `trigger(availableNow=True)` × `once` × contínuo?</b></summary>

availableNow processa o backlog em vários lotes (respeita maxFilesPerTrigger) e para — ideal para job
agendado; once faz um lote só (risco de memória); processingTime/contínuo mantém o cluster ligado.

</details>

<details>
<summary><b>3. Por que não inferir o schema?</b></summary>

Custo de leitura, instabilidade entre lotes, mudança silenciosa. Envelope explícito, payload bruto (string ou
VARIANT) e evolução controlada (`schemaEvolutionMode`, rescued data).

</details>

<details>
<summary><b>4. O que acontece com uma linha JSON corrompida no modo padrão?</b></summary>

PERMISSIVE: sem coluna de corrupção, vira linha toda nula (perda silenciosa — demonstrado). Com
`_corrupt_record`, o texto fica guardado; no Databricks, `badRecordsPath` e `rescuedDataColumn`.

</details>

<details>
<summary><b>5. A fonte adicionou um campo. O que acontece no seu pipeline?</b></summary>

Local com schema explícito: o campo é ignorado. Auto Loader: `addNewColumns` para o stream, evolui o schema e
segue no restart; `rescue` manda para `_rescued_data`; `failOnNewColumns` exige ação.

</details>

<details>
<summary><b>6. Directory listing ou file notification?</b></summary>

Listing é simples e serve até milhares de arquivos por lote; notification (Event Grid + Queue na Azure) escala
para milhões e reduz latência/custo de LIST. Com backfillInterval de segurança.

</details>

<details>
<summary><b>7. Como você faz backfill sem duplicar?</b></summary>

Não apagando checkpoint sobre alvo append (duplica — demonstrado). Camada seguinte idempotente (MERGE) ou batch
com `replaceWhere` no intervalo.

</details>

<details>
<summary><b>8. COPY INTO ou Auto Loader?</b></summary>

COPY INTO para carga SQL simples e poucos arquivos; Auto Loader para ingestão incremental de produção, muitos
arquivos, evolução de schema e Lakeflow.

</details>

<details>
<summary><b>9. Por que o download grava em `.tmp` e renomeia?</b></summary>

Para a ingestão nunca ver arquivo parcial; rename é atômico no mesmo filesystem e no ADLS Gen2 com HNS. Em
object store sem rename atômico: pasta de staging ou marcador `_SUCCESS`.

</details>

<a id="04-ingestao-api-incremental"></a>

## 04 · Ingestão de API REST incremental (GitHub)

Notebook: [`04_ingestao_api_incremental.ipynb`](notebooks/04_ingestao_api_incremental.ipynb)

<details>
<summary><b>1. Como você ingere uma API paginada sem perder nem duplicar registros?</b></summary>

Sigo o header <code>Link</code> (de preferência cursor, não offset), ordeno por atualização crescente, gravo cada página
bruta na landing, avanço a marca d'água só depois da landing completa e consolido com MERGE por chave estável.
Duplicata é absorvida pelo MERGE; perda é evitada pela ordem dado → estado e pelo cursor.

</details>

<details>
<summary><b>2. Offset × cursor: qual a diferença e quando o offset quebra?</b></summary>

Offset pede "a partir da posição N"; se a lista muda durante a leitura, itens mudam de posição e um pode ser pulado
ou repetido. Cursor (keyset) pede "depois da chave X" e é estável. Offset também fica lento em páginas profundas.

</details>

<details>
<summary><b>3. O que é idempotência num pipeline e como você garante?</b></summary>

Rodar 2× produz o mesmo resultado que 1×. Garanto com escrita por MERGE em chave natural (ou sobrescrita de
partição determinística), estado gravado depois do dado e dedupe da origem. Isso torna seguro o retry do orquestrador.

</details>

<details>
<summary><b>4. Full load × incremental: como decidir?</b></summary>

Incremental quando o volume é grande e há campo confiável de mudança (<code>updated_at</code>, sequência, CDC). Full quando a
tabela é pequena, a fonte não tem marca confiável ou preciso de deletes. Na prática: incremental diário +
reconciliação full periódica.

</details>

<details>
<summary><b>5. Por que gravar a resposta bruta antes de transformar?</b></summary>

Para reprocessar sem chamar a API de novo (cota, dado que pode ter mudado ou sumido), auditar o que a fonte de fato
respondeu e separar falha de ingestão de falha de transformação.

</details>

<details>
<summary><b>6. Um 304 com ETag gasta cota no GitHub?</b></summary>

Medido aqui: sem token, gastou. A documentação isenta o 304 de requisições autenticadas. O ETag sempre economiza
corpo e processamento; a isenção de cota depende de autenticar.

</details>

<details>
<summary><b>7. A API respondeu 429 / 403 de limite secundário. O que seu código faz?</b></summary>

Lê <code>Retry-After</code> e espera exatamente isso; sem o header, backoff exponencial com jitter. Se for o limite primário
esgotado com reset distante, levanta exceção específica e o orquestrador reagenda — não dorme segurando cluster.

</details>

<details>
<summary><b>8. Como você faz backfill de 2 anos numa API com cota?</b></summary>

É o mesmo incremental com a marca reposicionada, em fatias que cabem na cota (freio de páginas por execução), com
chave de estado própria para não atrapalhar a carga diária; token de App para cota maior; e idempotência para
poder parar e retomar a qualquer momento.

</details>

<details>
<summary><b>9. Onde fica o token da API no Databricks/Azure?</b></summary>

No Key Vault, exposto por secret scope apoiado no cofre; o código lê com <code>dbutils.secrets.get</code> e o valor sai
mascarado em logs. Nunca em notebook, repositório ou variável de cluster em texto puro.

</details>

<details>
<summary><b>10. Como incremental por <code>updated_at</code> lida com registros apagados?</b></summary>

Não lida — delete não altera <code>updated_at</code> de nada. Precisa de reconciliação full periódica (anti-join entre
fonte e destino), de evento de delete (webhook) ou de CDC na origem.

</details>

<a id="05-bronze-silver-merge-scd2"></a>

## 05 · Bronze → Silver: tipagem, deduplicação, MERGE e SCD tipo 2

Notebook: [`05_bronze_silver_merge_scd2.ipynb`](notebooks/05_bronze_silver_merge_scd2.ipynb)

<details>
<summary><b>1. O que torna um pipeline idempotente? Como você prova?</b></summary>

Reexecutar com a mesma entrada produz o mesmo estado final. Aqui: checkpoint (não relê) + MERGE por chave
(se reler, não duplica) + UPDATE condicionado a hash (não reescreve sem mudança). Prova: reprocessar com
checkpoint novo e mostrar mesma contagem, mesmos ids distintos e nenhuma versão nova no log (seção 3).

</details>

<details>
<summary><b>2. `dropDuplicates` ou `row_number`?</b></summary>

Mesma contagem, garantias diferentes: `dropDuplicates` mantém uma linha arbitrária; `row_number` com
ordem explícita escolhe a correta (a mais recente, a de maior sequência). Em streaming,
`dropDuplicatesWithinWatermark` limita o estado.

</details>

<details>
<summary><b>3. Seu MERGE está lento. Por onde começa?</b></summary>

`DESCRIBE HISTORY` → `operationMetrics`: `scanTimeMs` alto = busca varrendo tudo (falta predicado podável na
condição ou dado não agrupado pela chave); `numTargetRowsCopied` alto = copy-on-write de arquivos grandes
(ligar deletion vectors, arquivos menores). Depois: fonte deduplicada e pequena (broadcast), Liquid
Clustering na chave, concorrência entre writers.

</details>

<details>
<summary><b>4. O que são deletion vectors e qual o custo?</b></summary>

Bitmap por arquivo marcando linhas apagadas/atualizadas: a escrita não reescreve o Parquet (merge-on-read).
Custo: a leitura aplica o bitmap; precisa de OPTIMIZE/purge periódico; leitores antigos não suportam.
Aqui: para atualizar 3 linhas, `numTargetRowsCopied` caiu de ~186 mil (a tabela inteira) para 0.

</details>

<details>
<summary><b>5. Explique SCD1, SCD2 e SCD3 com um exemplo.</b></summary>

Repo transferido de conta: SCD1 guarda só o dono atual; SCD2 guarda uma linha por dono com
`valid_from`/`valid_to`/`is_current`; SCD3 guarda atual + anterior em colunas. Escolha de negócio: atribuir
a atividade passada ao dono de hoje (1) ou ao de então (2).

</details>

<details>
<summary><b>6. Como sua SCD2 lida com dado que chega fora de ordem?</b></summary>

Recalcula a linha do tempo das chaves afetadas (histórico ∪ novo, ordenado por data efetiva, colapsando
repetições) e aplica a diferença com MERGE (insert/update/delete). Demonstrado com a hora 11 chegando
depois das 12–14 e num caso sintético de mudança no meio.

</details>

<details>
<summary><b>7. Surrogate key: sequencial, identity ou hash?</b></summary>

Sequencial/identity é compacta mas não determinística (rebuild muda as chaves). Hash de (chave natural,
valid_from) é estável e paralelizável; risco de colisão em 64 bits é desprezível no volume típico.

</details>

<details>
<summary><b>8. `foreachBatch` é exactly-once?</b></summary>

Não por si só — é at-least-once (o lote pode rodar de novo após falha). Fica efetivamente exactly-once se
a escrita for idempotente: MERGE por chave, ou `txnAppId`/`txnVersion` no writer Delta.

</details>

<details>
<summary><b>9. Por que a condição do MERGE tem a data como literal se já casa por `event_id`?</b></summary>

Predicado só do alvo, com valor conhecido, permite podar partições/arquivos na fase de busca. Igualdade
com a coluna da fonte (`t.event_date = s.event_date`) não poda nada no Delta OSS.

</details>

<details>
<summary><b>10. Quando NÃO usar SCD2?</b></summary>

Atributo volátil (muda todo dia) — vira fato/snapshot; ninguém consulta o passado — SCD1 basta; histórico
curto para auditoria — time travel do Delta (com retenção) pode resolver.

</details>

<a id="06-streaming-structured-streaming"></a>

## 06 · Streaming com Structured Streaming (Wikimedia EventStreams)

Notebook: [`06_streaming_structured_streaming.ipynb`](notebooks/06_streaming_structured_streaming.ipynb)

<details>
<summary><b>1. Como Structured Streaming garante exactly-once?</b></summary>

Fonte replayable (reler um intervalo de offsets), checkpoint com write-ahead log de offsets e commit por micro-lote,
e sink idempotente (Delta guarda <code>(queryId, batchId)</code> e ignora regravação). Falha no meio → o mesmo lote é
refeito sem duplicar.

</details>

<details>
<summary><b>2. O que é watermark e o que acontece com evento que chega depois dele?</b></summary>

Máximo event time visto menos o atraso tolerado. Evento mais velho é descartado (métrica
<code>numRowsDroppedByWatermark</code>), e janelas que terminaram antes dele são finalizadas e saem do estado.

</details>

<details>
<summary><b>3. Event time × processing time?</b></summary>

Event time é quando o fato ocorreu (vem no dado); processing time é quando o cluster processou. Agregação de negócio
usa event time para o resultado não depender de quando o job rodou.

</details>

<details>
<summary><b>4. Diferença entre os output modes append, update e complete?</b></summary>

Append: só linhas finais (com agregação, exige watermark). Update: só linhas alteradas no lote (casa com MERGE).
Complete: a tabela de resultado inteira a cada lote (só para resultado pequeno).

</details>

<details>
<summary><b>5. Por que meu stream em append com janela não escreve as últimas janelas?</b></summary>

Porque elas só saem quando o watermark passa do fim delas, e o watermark só avança com dado novo. Com
<code>availableNow</code> o stream termina antes; elas ficam no estado até a próxima execução.

</details>

<details>
<summary><b>6. <code>foreachBatch</code> é exactly-once?</b></summary>

Não por si: é at-least-once — o mesmo <code>batchId</code> pode rodar de novo após falha. Fica exactly-once se a escrita for
idempotente (MERGE com valor absoluto, ou <code>txnAppId</code>/<code>txnVersion</code> no append Delta).

</details>

<details>
<summary><b>7. Quando usar <code>availableNow</code> em vez de um stream contínuo?</b></summary>

Quando latência de minutos/horas basta: agenda o job, ele processa o incremental e desliga — mesmas garantias,
custo de batch. Contínuo só quando a latência paga o cluster ligado.

</details>

<details>
<summary><b>8. Meu job de streaming está ficando para trás. Como você diagnostica?</b></summary>

No progress: input rate acima de processed rate, <code>triggerExecution</code> maior que o intervalo, estado crescendo.
Causas: skew, estado sem watermark, lote grande demais, sink lento (MERGE sem clustering). Ações: escalar, limitar
taxa, corrigir watermark, RocksDB, clusterizar o destino.

</details>

<details>
<summary><b>9. Para que serve o RocksDB como state store?</b></summary>

Guardar estado grande fora do heap da JVM (em disco local, com cache), evitando GC longo e OOM; com changelog
checkpointing o checkpoint por lote fica incremental.

</details>

<details>
<summary><b>10. Como ler Azure Event Hubs no Spark?</b></summary>

Pelo endpoint compatível com Kafka (porta 9093, SASL_SSL): <code>format("kafka")</code> com o namespace como bootstrap e o
event hub como tópico; credencial do Key Vault ou, melhor, Entra ID. Fonte replayable dentro da retenção.

</details>

<details>
<summary><b>11. Como deduplicar eventos num stream sem estado infinito?</b></summary>

<code>withWatermark</code> + <code>dropDuplicatesWithinWatermark(["event_id"])</code>: cada id fica no estado só enquanto o watermark
não passou. <code>dropDuplicates</code> sem watermark guarda todos os ids para sempre.

</details>

<a id="07-gold-modelagem-dimensional"></a>

## 07 · Gold: modelagem dimensional (Kimball), star schema e Liquid Clustering

Notebook: [`07_gold_modelagem_dimensional.ipynb`](notebooks/07_gold_modelagem_dimensional.ipynb)

<details>
<summary><b>1. O que é o grão de uma tabela fato e por que é a primeira decisão?</b></summary>

É o que uma linha representa ("um evento", "um repo por dia"). Define quais dimensões cabem, quais medidas
somam e protege contra joins que multiplicam linhas. Aqui, o join só por `repo_id` com a SCD2 inflou o fato.

</details>

<details>
<summary><b>2. Surrogate key × natural key?</b></summary>

Natural vem da fonte; surrogate é do DW, sem significado, e identifica uma VERSÃO (necessária na SCD2). Gero
por hash determinístico de (chave natural, valid_from) para rebuild não mudar as chaves.

</details>

<details>
<summary><b>3. Como você liga o fato à versão certa de uma dimensão SCD2?</b></summary>

Join point-in-time: mesma chave natural e `valid_from <= ts < valid_to`, resolvido na carga do fato, que grava
a sk. Consultas depois usam só a sk (join de igualdade, barato).

</details>

<details>
<summary><b>4. Transacional, snapshot periódico e acumulativo — dê um exemplo de cada.</b></summary>

Evento do GitHub; atividade por repo × dia; ciclo de vida do PR (aberto/revisado/mergeado na mesma linha,
atualizada por MERGE).

</details>

<details>
<summary><b>5. O que é dimensão conformada?</b></summary>

Mesma dimensão compartilhada por vários fatos (dim_date, dim_repo), permitindo comparar e cruzar métricas
("drill across") sem divergência de definição.

</details>

<details>
<summary><b>6. Medida aditiva × não aditiva — como você trata atores distintos e taxas?</b></summary>

Não somar entre grãos: recalcular do detalhe, guardar numerador/denominador, ou sketches (HLL). Demonstrado:
soma de distintos por repo é muito maior que os distintos reais.

</details>

<details>
<summary><b>7. OBT ou star schema?</b></summary>

Star para integração e governança; OBT derivada para servir BI com zero joins. OBT sozinha duplica lógica e
precisa ser reescrita quando dimensão muda.

</details>

<details>
<summary><b>8. Particionaria a Gold? Por quê?</b></summary>

Só com partições de ~1 GB+ e filtro sempre pela coluna. Caso contrário, Liquid Clustering nas colunas de
filtro/join e OPTIMIZE. Aqui, o fato de 3 horas tem ~7 MB — não particiono.

</details>

<details>
<summary><b>9. Liquid Clustering × Z-order?</b></summary>

Liquid é incremental, as colunas mudam sem reescrever, dispensa partição. Z-order reescreve a partição toda e
depende do esquema de partição. Liquid roda no Delta OSS (testado); `clusterBy` do DataFrameWriter por caminho é ignorado.

</details>

<details>
<summary><b>10. Quando Data Vault?</b></summary>

Muitas fontes, auditoria forte, modelo que muda — como camada de integração. Por cima, ainda um dimensional
para consumo.

</details>

<a id="08-qualidade-e-contratos"></a>

## 08 · Qualidade de dados e contratos: expectations, quarentena, freshness, volume e pipelines declarativos

Notebook: [`08_qualidade_e_contratos.ipynb`](notebooks/08_qualidade_e_contratos.ipynb)

<details>
<summary><b>1. Qual a diferença entre warn, drop e fail? Quando usa cada uma?</b></summary>

warn mede e deixa passar (regra nova, anomalia tolerável); drop remove a linha (para quarentena, com motivo)
quando o problema é do registro; fail aborta o lote quando a violação torna tudo suspeito (chave nula, schema).

</details>

<details>
<summary><b>2. Por que quarentena e não simplesmente filtrar?</b></summary>

Filtro apaga em silêncio. Quarentena preserva a linha e o motivo, permite medir, alertar, corrigir na fonte e
reprocessar. Precisa de dono e retenção.

</details>

<details>
<summary><b>3. O que é um data contract e como você o aplica?</b></summary>

Acordo versionado produtor × consumidor (colunas, tipos, nulos, chave, grão, freshness, dono). Valido o schema
real contra o YAML antes de publicar; quebra = job do produtor falha. Mudança incompatível = versão major.

</details>

<details>
<summary><b>4. Como você detecta que uma fonte parou de mandar dados?</b></summary>

Freshness: max(timestamp do evento) vs agora contra o SLA, num monitor independente do job de carga (se o
job nem roda, o monitor ainda alerta).

</details>

<details>
<summary><b>5. E um arquivo que chegou pela metade?</b></summary>

Checagem de volume contra o histórico (z-score leave-one-out, ou mesma hora da semana anterior ±X%).
Demonstrado: hora com 30% do volume vira anomalia.

</details>

<details>
<summary><b>6. Como uma condição com NULL se comporta numa expectation?</b></summary>

`NULL > 0` é NULL. Aqui NULL conta como falha (explícito); em CHECK constraint SQL, NULL passa. Por isso regra
de não nulo separada.

</details>

<details>
<summary><b>7. Expectations do Lakeflow guardam as linhas descartadas?</b></summary>

Não — `expect_or_drop` descarta e só registra a contagem no event log. Para quarentena: segunda tabela com a
condição invertida, ou DQX.

</details>

<details>
<summary><b>8. O Spark open source tem pipelines declarativos?</b></summary>

Sim, desde o 4.1: `pyspark.pipelines` + CLI `spark-pipelines` (sobre Spark Connect). Rodei aqui. Mas sem
expectations — é o núcleo do DLT, não o produto inteiro.

</details>

<details>
<summary><b>9. Great Expectations, Soda, DQX ou dbt tests?</b></summary>

Depende de onde está a transformação: Lakeflow/DQX dentro do Databricks; dbt tests se o time usa dbt; Soda/GE
para checagens agendadas e relatório fora do pipeline. Constraints Delta para invariantes.

</details>

<details>
<summary><b>10. Como você evita que regras de qualidade virem alarme que todo mundo ignora?</b></summary>

Toda regra tem dono e ação; regra nova entra como warn e é calibrada; alerta por taxa/tendência, não por
linha; métricas históricas para ver regressão; revisar regras que nunca falham ou sempre falham.

</details>

<a id="09-performance-spark"></a>

## 09 · Performance no Spark

Notebook: [`09_performance_spark.ipynb`](notebooks/09_performance_spark.ipynb)

<details>
<summary><b>1. O que é lazy evaluation e qual a vantagem?</b></summary>

Transformações só montam o plano; a action executa. O
Catalyst otimiza o plano inteiro (pushdown, pruning, escolha de join) antes de rodar. Custo:
reusar o DataFrame recalcula tudo (§1).

</details>

<details>
<summary><b>2. Diferença entre transformação narrow e wide? Onde começa um stage novo?</b></summary>

Narrow: cada partição de saída depende de uma de entrada
(filter, select) — mesmo stage. Wide: precisa reunir chaves de todas as partições (groupBy,
join) — exige shuffle, que aparece como `Exchange` e abre stage novo (§1).

</details>

<details>
<summary><b>3. Um job travou em 199/200 tasks. O que você investiga?</b></summary>

Skew. Na Spark UI: Summary Metrics do stage (max ≫
mediana em duração e shuffle read), qual chave domina (`groupBy(key).count()`), chave nula.
Conserto: AQE skew join, broadcast se a dimensão couber, salting nas chaves quentes, tratar a
chave à parte (§5).

</details>

<details>
<summary><b>4. Broadcast hash join × sort-merge join: quando cada um?</b></summary>

BHJ quando um lado cabe na memória dos executores
(dimensão): sem shuffle do lado grande. SMJ para dois lados grandes. Automático abaixo de
`autoBroadcastJoinThreshold` (10 MB), hint para forçar; AQE troca em runtime (§3b, §4).

</details>

<details>
<summary><b>5. O que o AQE faz e o que ele não faz?</b></summary>

Faz: coalesce de partições, troca para broadcast, divisão
de partições com skew em join — tudo a partir do tamanho real do shuffle. Não faz: nada sem
shuffle (scan de small files), skew em janela, e mede skew em bytes, não em linhas (§3, §5).

</details>

<details>
<summary><b>6. Como escolher `spark.sql.shuffle.partitions`?</b></summary>

Partições de ~100–200 MB, múltiplo do total de cores; na
prática, valor alto + AQE coalescendo. 200 é padrão arbitrário (§2).

</details>

<details>
<summary><b>7. Por que small files são um problema e como resolver?</b></summary>

Overhead por arquivo (listar, abrir, rodapé, entrada no
log; no ADLS, requisições HTTP) domina. Prevenir (micro-lotes maiores, optimized writes, não
over-particionar) e compactar (`OPTIMIZE`, auto compaction, Predictive Optimization) (§6).

</details>

<details>
<summary><b>8. Partição, Z-order ou Liquid Clustering?</b></summary>

Partição: baixa cardinalidade, sempre filtrada, ≥ 1 GB por
partição. Z-order: clustering por OPTIMIZE, não incremental. Liquid: incremental, chaves
mutáveis, substitui os dois em tabela nova; `CLUSTER BY AUTO` no Databricks (§7, §8).

</details>

<details>
<summary><b>9. Como provar que o filtro foi empurrado para a leitura?</b></summary>

`explain("formatted")`: `PushedFilters`, `PartitionFilters`,
`ReadSchema`; e na métrica do scan, arquivos lidos × total. UDF ou cast na coluna quebram (§9).

</details>

<details>
<summary><b>10. Quando usar cache?</b></summary>

Resultado caro reusado várias vezes na mesma sessão. Não em
pipeline de uma passada: ocupa memória de execução (spill) e fica velho se a fonte muda.
`unpersist` no fim (§10).

</details>

<details>
<summary><b>11. Por que UDF Python é lenta e quais as alternativas?</b></summary>

Serializa linha a linha JVM ↔ Python, quebra codegen e
pushdown, não roda no Photon. Alternativas: função nativa, funções de alta ordem, pandas UDF ou
UDF com Arrow (§11).

</details>

<details>
<summary><b>12. Spill e OOM: qual a diferença e como tratar?</b></summary>

Spill: task sem memória de execução despeja em disco e fica
lenta. OOM no executor: partição grande demais para caber nem com spill (skew, explode). OOM no
driver: `collect`/broadcast grande. Remédio comum: partições menores; no driver, não trazer
volume (§12, §13).

</details>

<a id="10-delta-lake-por-dentro"></a>

## 10 · Delta Lake por dentro

Notebook: [`10_delta_lake_por_dentro.ipynb`](notebooks/10_delta_lake_por_dentro.ipynb)

<details>
<summary><b>1. O que é uma tabela Delta, fisicamente?</b></summary>

Arquivos parquet + a pasta `_delta_log` com um JSON por
commit. Cada JSON tem ações (`add`, `remove`, `metaData`, `protocol`, `commitInfo`); o estado
da tabela é o replay delas. A cada 10 commits, um checkpoint parquet consolida o estado (§1, §2).

</details>

<details>
<summary><b>2. Como o Delta garante ACID em cima de object storage?</b></summary>

Atomicidade: o commit é a criação atômica de um arquivo do
log (put-if-absent). Isolamento: leitores fixam uma versão (snapshot); escritores usam
concorrência otimista e validam no commit. Consistência: enforcement de schema e constraints.
Durabilidade: o storage (§1, §3).

</details>

<details>
<summary><b>3. Dois jobs escrevem na mesma tabela ao mesmo tempo. O que acontece?</b></summary>

Não há lock. Quem commita depois confere se o vencedor
mexeu no que ele leu: se não, recommita sozinho; se sim, falha com
`ConcurrentAppendException`/`ConcurrentDeleteReadException`. Appends cegos nunca conflitam.
Evita-se com partições disjuntas + partição explícita na condição; trata-se com retry (§3).

**4. Meu MERGE falha com `ConcurrentAppendException` mesmo atualizando linhas diferentes do
outro job. Por quê?**
A condição do MERGE não restringe a partição, então ele
"leu a tabela inteira"; qualquer arquivo novo de outro job conflita. Conserto: incluir a
coluna de partição na condição (`AND t.event_hour = 12`). No Databricks, row-level concurrency
reduz o problema ☁️ (§3).

</details>

<details>
<summary><b>5. O que o VACUUM faz e por que a retenção padrão é de 7 dias?</b></summary>

Apaga do storage arquivos fora da versão atual e mais
velhos que a retenção. 7 dias protegem leitores longos e escritas não commitadas (retenção 0
pode apagar arquivo de transação em andamento e corromper a tabela) e dão janela de time
travel. Depois dele, ler a versão antiga falha (§5).

</details>

<details>
<summary><b>6. Time travel substitui backup?</b></summary>

Não. Depende dos arquivos antigos e do log, que somem com
VACUUM (7 dias) e com a limpeza do log (30 dias); e não protege contra perda do storage ou
exclusão da pasta. Serve para auditoria, reprodutibilidade e desfazer erro recente com
`RESTORE` (§4, §5).

</details>

<details>
<summary><b>7. Diferença entre schema enforcement e schema evolution?</b></summary>

Enforcement: escrita com schema diferente falha (padrão).
Evolution: opt-in (`mergeSchema`, `WITH SCHEMA EVOLUTION`) para mudança compatível — coluna
nova, tipo mais largo com type widening. Renomear/dropar exige column mapping. Permissivo na
bronze, estrito na silver/gold (§6, §8).

</details>

<details>
<summary><b>8. O que são deletion vectors e o que mudam?</b></summary>

Bitmap de linhas apagadas por arquivo. DELETE/UPDATE/MERGE
deixam de reescrever o parquet inteiro: marcam no DV e gravam só as linhas novas
(merge-on-read). Escrita pontual barata, leitura com um filtro a mais, protocolo leitor 3 /
escritor 7, e a linha segue no arquivo até `REORG PURGE` + VACUUM (§9).

</details>

<details>
<summary><b>9. Como propagar UPDATE e DELETE da silver para a gold sem reprocessar tudo?</b></summary>

Change Data Feed: ler as mudanças desde a última versão
processada (batch com `startingVersion` ou streaming com checkpoint), reduzir a uma linha por
chave e aplicar com MERGE idempotente (§10).

</details>

<details>
<summary><b>10. Como apagar definitivamente os dados de uma pessoa (LGPD)?</b></summary>

`DELETE` tira do estado atual, mas o dado segue nos
arquivos antigos (time travel) e, com DV, no próprio arquivo. Sequência: DELETE →
`REORG ... APPLY (PURGE)` → `VACUUM` após a retenção — e propagar para camadas derivadas,
clones e CDF (§5, §8, §9).

</details>

<details>
<summary><b>11. O que é o protocolo da tabela e por que eu deveria me preocupar ao ligar uma feature?</b></summary>

`minReaderVersion`/`minWriterVersion` + table features: o
que o cliente precisa suportar. Ligar DV ou column mapping sobe o protocolo de leitura e barra
clientes antigos; voltar atrás é difícil. Conferir quem lê a tabela antes (§11).

</details>

<details>
<summary><b>12. Delta ou Iceberg?</b></summary>

Mesmo problema, mecanismos diferentes (log ordenado ×
árvore de metadados com commit no catálogo). Em Databricks, Delta é o nativo; para outros
motores, UniForm/Iceberg REST do Unity Catalog expõe a mesma tabela sem duplicar dado. A
decisão hoje é mais de ecossistema e catálogo que de formato (§13).

</details>

<a id="11-governanca-unity-catalog-lgpd"></a>

## 11 · Governança, Unity Catalog e LGPD

Notebook: [`11_governanca_unity_catalog_lgpd.ipynb`](notebooks/11_governanca_unity_catalog_lgpd.ipynb)

<details>
<summary><b>1. Explique a hierarquia do Unity Catalog e o que é o namespace de três níveis.</b></summary>

Metastore (um por região, no nível da conta) → catalog → schema → objeto (table, view, volume, function,
model). Todo objeto é referenciado como <code>catalogo.schema.objeto</code>. Storage credential e external
location ficam direto no metastore. Permissão, tag, lineage e auditoria são centrais e valem em todos os
workspaces ligados ao metastore. Local não existe: o <code>spark_catalog</code> tem dois níveis e nem aceita
<code>GRANT</code> (§1).

</details>

<details>
<summary><b>2. Managed × external table: qual a diferença prática e qual você escolhe?</b></summary>

Managed: o catálogo é dono dos arquivos; <code>DROP</code> apaga o dado (no UC, depois de 7 dias de
<code>UNDROP</code>) e a plataforma faz a manutenção. External: eu informo o <code>LOCATION</code>;
<code>DROP</code> só tira o registro e os arquivos ficam — provado no §2. Padrão managed; external quando outro
sistema usa o mesmo caminho. Para LGPD, "dropei a tabela externa" não eliminou nada.

</details>

<details>
<summary><b>3. Um analista tem <code>SELECT</code> na tabela e mesmo assim recebe erro de permissão. Por quê?</b></summary>

Falta <code>USE CATALOG</code> no catálogo ou <code>USE SCHEMA</code> no schema: são pré-requisitos para
"atravessar" a hierarquia. Outras causas: o catálogo está com <em>workspace binding</em> e ele está em outro
workspace; o compute não tem modo de acesso do UC; ou há uma política ABAC em conflito (duas máscaras na mesma
coluna fazem a consulta falhar).

</details>

<details>
<summary><b>4. Por que "fazemos hash do CPF" não é anonimização? O que você faria?</b></summary>

Hash sem segredo é determinístico e o espaço de CPFs é pequeno e enumerável: o atacante calcula o hash de todos
os candidatos e faz um join (ataque de dicionário). No §5, com um dicionário de duas horas de dado público,
60,2% dos eventos foram reidentificados em segundos. Eu usaria HMAC-SHA256 com chave no Key Vault para o
pseudônimo de junção — e chamaria de pseudonimização: quem tem a chave reverte, então segue sendo dado pessoal
(art. 13 §4º). Sal conhecido não resolve; sal aleatório por linha quebra a junção.

</details>

<details>
<summary><b>5. Quando usar HMAC, tokenização, criptografia ou mascaramento?</b></summary>

HMAC: ninguém precisa reverter, mas preciso juntar e contar. Tokenização: um grupo restrito precisa reverter,
com auditoria — o cofre fica separado e precisa ser persistido (token aleatório não é recalculável).
Criptografia: preciso guardar o valor; não serve para join porque cada cifra é diferente (§6). Máscara: só
exibição parcial. Generalização/k-anonimato: publicar ou reter sem identificar, medindo o k.

</details>

<details>
<summary><b>6. Diferença entre dynamic view, row filter/column mask e ABAC. Qual é o recomendado hoje?</b></summary>

Dynamic view: <code>CASE WHEN is_account_group_member(...)</code> no SQL da view; o usuário consulta outro
objeto. Row filter/column mask: função SQL presa à tabela por <code>ALTER TABLE</code>; uma a uma. ABAC:
<code>CREATE POLICY</code> no catálogo ou schema, casando por tag governada — cobre tabelas futuras e o dono
da tabela não remove. Para regra corporativa a documentação recomenda ABAC (exige serverless ou DBR 16.4+).
A identidade do pipeline vai no <code>EXCEPT</code>, senão a tabela derivada é gravada mascarada.

</details>

<details>
<summary><b>7. Login do GitHub é público. Preciso me preocupar com LGPD?</b></summary>

Sim. É dado pessoal porque identifica uma pessoa natural; "público" descreve o acesso. O art. 7º §4º dispensa
o consentimento para dado tornado manifestamente público pelo titular, mas mantém os direitos do titular e os
princípios — finalidade, necessidade, segurança. Posso analisar atividade pública; não posso montar perfil
para outra finalidade, guardar para sempre nem ignorar pedido de eliminação.

</details>

<details>
<summary><b>8. Um titular pediu eliminação. Você rodou <code>DELETE</code>. Acabou?</b></summary>

Não. O <code>DELETE</code> é lógico: os arquivos antigos ficam e o time travel devolve o dado (§11). Falta
<code>VACUUM</code> depois da retenção; se a tabela tem deletion vectors (padrão no Databricks),
<code>REORG TABLE … APPLY (PURGE)</code> antes, senão o parquet com o dado continua na versão atual (§12).
E falta o resto: outras colunas e texto livre, landing, silver e versões de SCD, estatísticas no
<code>_delta_log</code>, soft delete do storage e backups (§13).

</details>

<details>
<summary><b>9. Por que o <code>VACUUM</code> tem retenção mínima de 7 dias e o que acontece se eu forçar zero?</b></summary>

Para proteger leitores longos, streams atrasados e, principalmente, escritas concorrentes: arquivo de transação
ainda não commitada não está no log e seria apagado, corrompendo a tabela. Forçar zero exige desligar
<code>retentionDurationCheck</code>; só em tabela sem concorrência, como demonstração. Efeito colateral
sempre: time travel para antes do <code>VACUUM</code> deixa de funcionar.

</details>

<details>
<summary><b>10. O que são deletion vectors e como afetam a LGPD?</b></summary>

Arquivos <code>.bin</code> que marcam linhas apagadas, evitando reescrever o parquet (merge-on-read). No §12
o <code>DELETE</code> gravou zero arquivos de dados e, depois do <code>VACUUM</code> com retenção zero, o
parquet ainda tinha todas as linhas do titular — porque o arquivo continua na versão atual. Só
<code>REORG … APPLY (PURGE)</code> + <code>VACUUM</code> remove.

</details>

<details>
<summary><b>11. Depois de DELETE, REORG e VACUUM, onde o dado ainda pode estar?</b></summary>

Nas estatísticas min/max do <code>_delta_log</code> (provado no §13; evita-se com
<code>delta.dataSkippingStatsColumns</code>); em valores de partição; em outras colunas e em texto livre; no
landing; em tabelas derivadas e em versões de SCD2; no state store e em cofres de token; em cache do cluster;
no soft delete/versionamento do ADLS; em backups e clones; em cópias de dev e exportações.

</details>

<details>
<summary><b>12. O que é crypto-shredding e quando vale a pena?</b></summary>

Cifrar o dado pessoal com uma chave por titular e, no pedido de eliminação, destruir a chave: todas as cópias,
inclusive backups, ficam ilegíveis sem reescrever nada (§13: a tabela de eventos ficou na versão 0). Vale
quando reescrever tudo é inviável (backups imutáveis, muitas cópias). Custos: tem de ser desenhado desde o
início, toda leitura do valor faz join com o cofre, o backup das chaves precisa da mesma disciplina, e o
pseudônimo determinístico que ficar ainda liga as linhas.

</details>

<a id="12-ia-aplicada-engenharia-de-dados"></a>

## 12 · IA aplicada à engenharia de dados

Notebook: [`12_ia_aplicada_engenharia_de_dados.ipynb`](notebooks/12_ia_aplicada_engenharia_de_dados.ipynb)

<details>
<summary><b>1. Como você garante que uma etapa com LLM é reproduzível?</b></summary>

Não tento fazer o modelo repetir a resposta; eu gravo a resposta. Toda chamada passa por um cache cuja chave é
o hash de modelo + versão do prompt + entrada + schema. Reprocessar lê do cache. A tabela de saída guarda
modelo, versão do prompt e chave por linha, então sei exatamente o que gerou cada valor (§1, §8).

</details>

<details>
<summary><b>2. Como você sabe se a coluna gerada pelo LLM presta?</b></summary>

Gabarito rotulado, baseline simples e métrica com intervalo de confiança. No caso dos títulos: 120 exemplos,
LLM contra palavras-chave, matriz de confusão e recortes por dificuldade. E digo as limitações: amostra pequena
e rótulo feito com apoio de IA medem concordância, não verdade (§4).

</details>

<details>
<summary><b>3. Quanto custa classificar 1 milhão de registros? Como você reduz?</b></summary>

Tokens por item (entrada e saída, medidos no gabarito) × volume × preço. Reduzo, nesta ordem: regra antes do
modelo (cascata), vários itens por chamada, modelo menor, Batch API (50%), cache por hash. E meço em produção
pelo `usage` da API — número de ferramenta de desenvolvimento superestima (§5, §6).

</details>

<details>
<summary><b>4. O dado tem PII. Pode mandar para um LLM?</b></summary>

O padrão é não mandar: mascaro antes. Para classificar colunas, o modelo vê formato, não valor. Para texto
livre, removo padrões conhecidos e assumo que regex não pega tudo. Se a tarefa exige o dado em claro, o modelo
tem de estar dentro do perímetro (mesma nuvem e região, contrato adequado) — decisão de segurança e jurídico
(§2, §3).

</details>

<details>
<summary><b>5. O que é prompt injection num pipeline de dados e como você se protege?</b></summary>

O dado contém instruções para o modelo — um título de issue, um comentário, uma mensagem de erro. Não confio em
pedir ao modelo que ignore: restrinjo a saída (enum validado), não dou ferramentas, nunca concateno texto do
modelo em SQL sem escapar e mantenho ataques no conjunto de avaliação. O ataque bem-sucedido vira, no máximo,
um rótulo errado (§7).

</details>

<details>
<summary><b>6. Como fazer inferência em bilhões de linhas sem estourar o rate limit?</b></summary>

Primeiro reduzo o problema: só linhas novas, só o que a regra não resolve, deduplicado por conteúdo. Depois,
`ai_query` (a plataforma gerencia) ou `mapInPandas` com pool de threads e token bucket, dimensionando partições
× threads pelo limite do provedor. Para volume grande sem pressa, Batch API e `MERGE` do resultado (§8).

</details>

<details>
<summary><b>7. Você deixaria um LLM criar regras de qualidade que param o pipeline?</b></summary>

Deixo ele **propor**. A regra é um JSON de tipos permitidos, validado e compilado por código nosso, e só roda
depois de aprovada por uma pessoa — sem decisão, é rejeitada. O revisor separa invariante de negócio de
acidente da amostra, e a severidade `fail` é decisão de quem responde pelo incidente (§9).

</details>

<details>
<summary><b>8. Quando você NÃO usaria LLM?</b></summary>

Quando existe regra estável, quando a tarefa é exata (cálculo, chave, join), quando o erro por item é caro e
não há revisão, quando volume × preço ou latência não fecham, quando o dado não pode sair e não há modelo
interno, ou quando não há como avaliar. E troco por modelo clássico quando já tenho rótulos suficientes (§6).

</details>

<details>
<summary><b>9. Como você versiona e faz deploy de um prompt?</b></summary>

Como código: arquivo no Git com id e versão, mudança por pull request, e um gate de CI que roda o gabarito
sobre respostas gravadas. Mudou o prompt, a chave do cache muda e o teste exige regravar — o PR mostra o prompt
novo e as respostas novas lado a lado (§12).

</details>

<details>
<summary><b>10. Qual a diferença entre usar o Genie Code / Copilot e ter IA no pipeline?</b></summary>

No assistente há sempre uma pessoa lendo a saída antes de ela valer; o controle é revisão de código e teste. No
pipeline ninguém está olhando: o controle precisa estar no desenho — contrato de saída, avaliação, gate,
monitoramento e pontos explícitos de aprovação humana (seção ☁️).

</details>

<details>
<summary><b>11. O gate de avaliação passa. O que ele NÃO garante?</b></summary>

Que o modelo acerta fora do gabarito, que o provedor não mudou o modelo por trás do mesmo nome, e que o dado de
produção se parece com o que eu rotulei. Por isso, além do gate: regravação periódica comparada, monitoramento
da distribuição dos rótulos e amostra auditada por humano (§12).

</details>

<details>
<summary><b>12. Onde fica o humano no circuito? Não vira gargalo?</b></summary>

Fica onde a saída **muda produção de forma durável**: regra de qualidade, comentário de catálogo, classificação
de PII. Aí a revisão é por tabela ou por regra — dezenas de itens, não milhões. Onde o volume é por linha
(classificação de títulos), não há humano por item: há gabarito, gate e auditoria por amostra (§9, §10).

</details>

<a id="13-cicd-git-asset-bundles"></a>

## 13 · Git, CI/CD e Declarative Automation Bundles

Notebook: [`13_cicd_git_asset_bundles.ipynb`](notebooks/13_cicd_git_asset_bundles.ipynb)

<details>
<summary><b>1. Trunk-based ou GitFlow para um time de dados? Por quê?</b></summary>

Trunk-based: branches curtas, PR pequeno, CI e deploy contínuo em dev. Branch longa diverge do schema e do dado de
produção. GitFlow só para produtos com várias versões suportadas em paralelo.

</details>

<details>
<summary><b>2. Rebase ou merge?</b></summary>

Rebase no que é local e não publicado (histórico limpo); nunca reescrever branch compartilhada. Na `main`, squash
ou rebase merge para histórico linear; `--force-with-lease` se for inevitável reescrever.

</details>

<details>
<summary><b>3. Um deploy quebrou produção. Revert ou reset?</b></summary>

`git revert` (commit novo, histórico preservado, CI e deploy rodam de novo). `reset` só local. E reverter o
código não reverte a tabela: `RESTORE` do Delta para o dado, se necessário.

</details>

<details>
<summary><b>4. Como você versiona um pacote e uma tabela?</b></summary>

Pacote: SemVer calculado de Conventional Commits, tag `vX.Y.Z`. Tabela: contrato versionado — coluna nova anulável
é MINOR; remover/mudar tipo é MAJOR com aviso, convivência (view/tabela v2) e data de remoção.

</details>

<details>
<summary><b>5. O que é um Declarative Automation Bundle (ex-Asset Bundle) e o que vai nele?</b></summary>

Definição em YAML dos recursos do Databricks junto com o código: `databricks.yml` (bundle, artefatos, variáveis,
alvos) e `resources/*.yml` (jobs, pipelines). O CLI valida, constrói, sobe e cria/atualiza por alvo.

</details>

<details>
<summary><b>6. Qual a diferença entre `mode: development` e `mode: production`?</b></summary>

Development: prefixo `[dev usuário]`, schedules pausados, concorrência liberada, sem lock. Production: valida
caminhos não pessoais, exige `run_as`/permissões, impede sobrescrever cluster; `run_as` com service principal.

</details>

<details>
<summary><b>7. Por que não declarar `pyspark` como dependência do wheel que vai para o Databricks?</b></summary>

O runtime já traz o Spark; o pip instalaria outro por cima e quebraria o cluster (ou mudaria a versão em
silêncio). `pyspark`/`delta-spark` ficam em grupo de desenvolvimento local.

</details>

<details>
<summary><b>8. Como o CI autentica no Databricks sem guardar token?</b></summary>

OIDC / workload identity federation: o GitHub emite token de curta duração; federation policy no service
principal aceita; `DATABRICKS_AUTH_TYPE=github-oidc`, `id-token: write`.

</details>

<details>
<summary><b>9. Como fica a pirâmide de testes num pipeline de dados?</b></summary>

Muitos unitários (funções e transformações com chispa), alguns de integração com Spark local e Delta
(idempotência), testes de dados/contrato em produção a cada lote, e um ponta a ponta no ambiente dev.

</details>

<details>
<summary><b>10. Por que este repositório versiona as saídas dos notebooks e não usa nbstripout?</b></summary>

As saídas são o produto (evidência legível no GitHub); a fonte revisável é o `.py` jupytext e o `.ipynb` é gerado
e executado por script. Em produção, o padrão é nbstripout para não vazar dado nem poluir o diff.

</details>

<details>
<summary><b>11. Retries: em quais tasks e quantos?</b></summary>

Só em tasks idempotentes (checkpoint, MERGE), poucos e com intervalo — cobrem falha transitória. Task de
qualidade não tem retry: reprovação é sinal. Junto, `max_concurrent_runs: 1` evita execuções sobrepostas.

</details>

<details>
<summary><b>12. Como você protege a `main`?</b></summary>

Ruleset: PR obrigatório com aprovação e CODEOWNERS, checks do CI obrigatórios, histórico linear, sem force push e
sem exclusão. pre-commit local para o barato; o CI repete porque o gancho local é opcional.

</details>

<a id="14-azure-terraform-azurite"></a>

## 14 · Azure com Terraform + Azurite (custo zero)

Notebook: [`14_azure_terraform_azurite.ipynb`](notebooks/14_azure_terraform_azurite.ipynb)

<details>
<summary><b>1. Por que ADLS Gen2 e não Blob Storage comum para o lakehouse?</b></summary>

O namespace hierárquico dá diretórios reais (rename/delete de pasta atômicos e rápidos), ACLs POSIX e o endpoint DFS
do driver ABFS. Para Spark/Delta isso significa operações de diretório O(1) em vez de copiar blob a blob.

</details>

<details>
<summary><b>2. Como o Databricks acessa o storage sem segredo?</b></summary>

Access Connector com identidade gerenciada + papel Storage Blob Data Contributor na conta; no Unity Catalog, uma
storage credential aponta para o connector e external locations definem os caminhos. Usuários recebem grants no
catálogo, nunca credenciais.

</details>

<details>
<summary><b>3. Managed identity × service principal × SAS × account key?</b></summary>

Managed identity: sem segredo, para recursos na Azure (preferida). Service principal: identidade de app com segredo
a rotacionar, para quem está fora. SAS: acesso delegado com escopo e prazo, para compartilhamento pontual. Account
key: acesso total sem rastreio por usuário — desligar.

</details>

<details>
<summary><b>4. Por que o workspace precisa ser premium?</b></summary>

Unity Catalog com controle de acesso fino, cluster policies, audit logs e outros recursos de governança exigem o
tier premium.

</details>

<details>
<summary><b>5. Como você organiza o Terraform para dev e prod?</b></summary>

Mesmo código, tfvars por ambiente, estado remoto separado por ambiente (backend azurerm com lock), versões de provider
fixadas, plan no PR e apply pelo pipeline autenticado por OIDC. Idealmente assinaturas separadas.

</details>

<details>
<summary><b>6. Onde ficam os segredos e como o notebook os lê?</b></summary>

No Key Vault (modo RBAC); o Databricks lê por secret scope apoiado no cofre com <code>dbutils.secrets.get</code>, e o valor
aparece mascarado. O ideal é eliminar segredos trocando por identidade gerenciada sempre que possível.

</details>

<details>
<summary><b>7. O que é VNet injection e private endpoint, e quando vale o custo?</b></summary>

VNet injection põe os clusters numa VNet sua (controle de rota, NSG, firewall de saída). Private endpoint dá IP
privado ao storage/Key Vault/workspace e permite desligar o acesso público. Vale em produção com dado sensível ou
exigência regulatória; em dev, firewall por IP costuma bastar.

</details>

<details>
<summary><b>8. Para que serve o Azurite e qual o limite dele?</b></summary>

Emular Blob Storage localmente para testar código (SDK, SAS, Spark via ABFS em modo FNS) sem custo. Não emula ADLS
Gen2 (HNS/DFS), TLS do abfss, RBAC/Entra ID nem rede — política de acesso se testa na nuvem.

</details>

<details>
<summary><b>9. Quais são os maiores custos de um lakehouse Databricks na Azure e como controlar?</b></summary>

DBU e VMs de compute. Controle: job compute em vez de all-purpose, cluster policy com limites e autodesligamento,
spot, serverless para cargas intermitentes, tags obrigatórias e acompanhamento por system tables de billing.
Storage pesa pouco, mas small files aumentam transações.

</details>

<details>
<summary><b>10. Um container por camada ou um container com pastas?</b></summary>

Com Unity Catalog, a permissão é no catálogo/external location, então um container com pastas basta e mantém o
layout do código; separo o storage gerenciado do catálogo e os ambientes (contas diferentes). Container por camada
volta a fazer sentido se ferramentas fora do Databricks acessam o storage direto.

</details>

<a id="15-observabilidade-e-custo"></a>

## 15 · Observabilidade e custo

Notebook: [`15_observabilidade_e_custo.ipynb`](notebooks/15_observabilidade_e_custo.ipynb)

<details>
<summary><b>1. Como você sabe que um pipeline está saudável?</b></summary>

Duas perguntas separadas. Execução: terminou, em quanto tempo, com que taxa de falha (tabela de execuções /
system tables). Dado: freshness, volume, schema e distribuição dentro do esperado. Job verde com dado errado é
o caso que só a segunda pega.

</details>

<details>
<summary><b>2. Quais são os pilares da observabilidade e como se aplicam a dados?</b></summary>

Métricas (duração, linhas, taxa de falha), logs (estruturados, com id de correlação) e traces (execução →
etapas → stages do Spark). Em dados somam-se lineage e os sinais do próprio dado: freshness, volume, schema,
distribuição.

</details>

<details>
<summary><b>3. Como registrar a execução de uma etapa sem esconder a falha?</b></summary>

Context manager com o registro no <code>finally</code>: grava status, duração e erro e relança a exceção. Se a
gravação do registro falhar, vai para o log e o erro original segue. Aqui: <code>track_step</code>, com teste
para os dois casos.

</details>

<details>
<summary><b>4. Como obter linhas gravadas sem rodar um `count()`?</b></summary>

No Delta, <code>operationMetrics</code> do commit (<code>numOutputRows</code>, <code>numTargetRowsInserted</code>…),
via <code>DESCRIBE HISTORY</code>. Em streaming, <code>numInputRows</code> do progresso. Dentro de uma
transformação, <code>df.observe</code>.

</details>

<details>
<summary><b>5. Diferença entre SLI, SLO e SLA? Dê um exemplo em dados.</b></summary>

SLI é a medida (idade do dado às 8h), SLO é a meta interna (menos de 2 h em 95% dos dias), SLA é o compromisso
com o consumidor, mais frouxo que o SLO. O orçamento de erro (1 − SLO) decide quando parar de entregar para
estabilizar.

</details>

<details>
<summary><b>6. Como monitorar um stream?</b></summary>

Pelo progresso de cada micro-lote (<code>StreamingQueryListener</code> ou <code>lastProgress</code>): linhas
processadas × recebidas por segundo, duração do lote × intervalo do gatilho, backlog da fonte, tamanho do
estado e watermark. Alerta quando o atraso persiste por vários lotes.

</details>

<details>
<summary><b>7. O job está verde, mas o dashboard mostra metade dos dados. Como você investiga?</b></summary>

Volume por carga na tabela de execuções e no <code>DESCRIBE HISTORY</code> (onde caiu?); lineage para subir
até a origem; <code>_source_file</code> para ver se faltou arquivo ou veio truncado; schema e distribuição para
ver se um filtro/join passou a descartar. Depois, a checagem de volume que teria pegado vira alerta.

</details>

<details>
<summary><b>8. O que o `DESCRIBE HISTORY` dá e o que ele não dá?</b></summary>

Dá a trilha de escrita: quem, quando, operação, parâmetros e métricas por commit, pelo período de retenção do
log. Não dá leitura (quem consultou) — isso é <code>system.access.audit</code> — nem alerta sozinho.

</details>

<details>
<summary><b>9. Como se calcula o custo de um job no Databricks?</b></summary>

DBUs (horas × nós × DBU/hora da VM) × preço do DBU do tipo de compute, mais a VM do provedor; em serverless a
VM está no DBU. Na prática: <code>system.billing.usage</code> × <code>list_prices</code> por
<code>usage_metadata.job_id</code>, e a VM no Azure Cost Management, juntando pelas tags.

</details>

<details>
<summary><b>10. A conta do Databricks dobrou este mês. O que você faz?</b></summary>

<code>system.billing.usage</code> por SKU, workspace, job e tag para achar o que cresceu; comparar com o mês
anterior. Suspeitos usuais: job em all-purpose, cluster interativo sem auto-termination, job que passou a
demorar mais (volume ou regressão), warehouse superdimensionado, consumo sem tag. Corrige e cria policy/alerta
para não voltar.

</details>

<details>
<summary><b>11. Quando usar Spot e quando não?</b></summary>

Workers de batch idempotente e sem prazo apertado: sim, com driver on-demand e fallback. Driver, streaming com
estado e job com SLA curto: não — a retomada custa recomputação e pode custar o SLA.

</details>

<details>
<summary><b>12. Photon sempre compensa?</b></summary>

Não. Ele emite mais DBU por hora; compensa quando o ganho de tempo supera esse fator — tipicamente joins e
agregações grandes em SQL/DataFrame. UDF Python, job curto ou dominado por I/O ganham pouco. Mede-se com a
carga real, com e sem.

</details>

<a id="16-live-coding-sql-pyspark"></a>

## 16 · Live coding: SQL e PySpark

Notebook: [`16_live_coding_sql_pyspark.ipynb`](notebooks/16_live_coding_sql_pyspark.ipynb)

<details>
<summary><b>1. `ROW_NUMBER`, `RANK` ou `DENSE_RANK` para top-N?</b></summary>

Depende de como tratar empate: exatamente N linhas → `ROW_NUMBER` com desempate determinístico; empates
incluídos → `RANK` (pula posições); N valores distintos → `DENSE_RANK`. Exercício 3.

</details>

<details>
<summary><b>2. Por que `NOT IN` devolveu zero linhas?</b></summary>

A subquery tem NULL: `x <> NULL` é desconhecido, e o `AND` de todas as comparações nunca é verdadeiro. Use
`NOT EXISTS` ou `LEFT ANTI JOIN`. Exercício 11.

</details>

<details>
<summary><b>3. Qual a diferença entre `ROWS` e `RANGE` numa janela? E o frame padrão?</b></summary>

`ROWS` conta linhas; `RANGE` usa o valor da chave de ordenação (dias faltando contam). Com `ORDER BY` e sem
frame, o padrão é `RANGE UNBOUNDED PRECEDING` — empates entram juntos. Exercício 4.

</details>

<details>
<summary><b>4. Como deduplicar mantendo o mais recente? E se houver empate no timestamp?</b></summary>

`ROW_NUMBER` por chave ordenado por timestamp desc **e** um desempate determinístico (sequência, offset, LSN);
ou `max_by(struct, struct(ts, seq))`. Exercício 1.

</details>

<details>
<summary><b>5. Explique gaps and islands.</b></summary>

Deduplica por dia, numera com `ROW_NUMBER` e subtrai do dia: valores consecutivos ficam com a mesma
diferença, que vira a chave do grupo. Exercício 6.

</details>

<details>
<summary><b>6. Como sessionizar cliques? E sessões que cruzam o limite do lote?</b></summary>

`LAG` → flag de nova sessão quando o intervalo passa do limite → soma acumulada. No limite do lote, reprocessar
com sobreposição ou usar `session_window` em streaming. Exercício 7.

</details>

<details>
<summary><b>7. Como trazer o atributo da dimensão vigente na data do fato?</b></summary>

SCD2 com `valid_from`/`valid_to` e join pelo intervalo semiaberto, ou as-of join (versão mais recente
≤ data do fato). Cuidado com `BETWEEN`, que duplica no dia da mudança. Exercício 8.

</details>

<details>
<summary><b>8. `percentile_approx` ou exato?</b></summary>

Aproximado usa memória fixa e escala; exato guarda todos os valores do grupo. O aproximado devolve um elemento
do conjunto (a mediana de uma quantidade par não interpola). Exercício 13.

</details>

<details>
<summary><b>9. Como você diferencia o NULL do subtotal do NULL do dado num `ROLLUP`?</b></summary>

`GROUPING(col)` = 1 na linha de subtotal; `GROUPING_ID` codifica o nível. Exercício 15.

</details>

<details>
<summary><b>10. Como achar o top-K de um arquivo cujas chaves não cabem na memória?</b></summary>

Particionar por hash estável em N arquivos, contar um por vez e manter um heap de K. Ou ordenação externa,
DuckDB/Spark, ou um sketch aproximado (Count-Min/Space-Saving). Exercício 18.

</details>

<details>
<summary><b>11. Seu `explode` "perdeu" linhas. Por quê?</b></summary>

`explode` descarta array vazio/nulo; use `explode_outer`. E `from_json` com JSON inválido vira NULL sem erro.
Exercício 10.

</details>

<details>
<summary><b>12. Sua consulta de janela é lenta numa tabela de 1 bilhão de linhas. O que você olha?</b></summary>

Se há `PARTITION BY` (sem ele, 1 tarefa só), skew na chave da partição (um ator com 11% dos eventos), se dá para
agregar antes da janela, e se a janela pode virar agregação (`max_by`) — no Spark UI, a duração das tarefas
do estágio da janela.

</details>

<a id="17-simulado-system-design"></a>

## 17 · Simulado de system design e troubleshooting

Notebook: [`17_simulado_system_design.ipynb`](notebooks/17_simulado_system_design.ipynb)

<details>
<summary><b>1. Desenhe um pipeline para 1 TB/dia. Por onde começa?</b></summary>

Pelos requisitos (consumidor, latência, retenção, PII) e pela conta: ~1 bilhão de eventos de 1 KB, ~12 mil/s
na média, ~50 mil/s no pico, centenas de TB em 2 anos. Só então barramento, bronze/silver/gold e o modo de
disparo. §2.

</details>

<details>
<summary><b>2. Streaming ou batch?</b></summary>

Pelo SLA e pelo custo: SLA de minutos → streaming (micro-lote); de horas → o mesmo código com
<code>trigger(availableNow)</code> agendado, cluster desligado entre execuções. Sub-segundo só com requisito real.

</details>

<details>
<summary><b>3. Como aplicar CDC com deletes e eventos fora de ordem?</b></summary>

Dedup por chave mantendo o maior LSN, <code>MERGE</code> com <code>s.lsn > t.lsn</code>, <code>WHEN MATCHED AND op='d' THEN DELETE</code>, e
soft delete/tombstone para um update atrasado não ressuscitar a linha. Reaplicar o lote não muda nada. §3.

</details>

<details>
<summary><b>4. Por que não usar marca d'água por `updated_at` para replicar o ERP?</b></summary>

Não enxerga delete físico, perde updates que não mexem no <code>updated_at</code> (trigger, carga em massa) e consulta as
tabelas de produção. CDC lê o log de transações.

</details>

<details>
<summary><b>5. Como você prova que a migração do DW está certa?</b></summary>

Reconciliação automática em níveis — contagem e checksum por partição, depois diff por chave nas partições
divergentes — em dual run por N dias, com o dono de negócio assinando. Só contagem esconde erros que se
compensam. §5.

</details>

<details>
<summary><b>6. O job ficou 3× mais lento sem mudança de código. O que você faz?</b></summary>

Gradual ou súbito? Comparo volume (<code>DESCRIBE HISTORY</code>), plano físico das duas execuções e o estágio que cresceu
(tarefa máx × mediana). Causas típicas: skew, small files, broadcast que virou sort-merge, cluster diferente.

</details>

<details>
<summary><b>7. Driver OOM ou executor OOM — como distingue e resolve?</b></summary>

Driver: <code>collect</code>/<code>toPandas</code>/broadcast grande/plano gigante → agregar no cluster, limitar, gravar em tabela.
Executor: partição grande (skew, poucas partições, explode) → mais partições, AQE, tratar skew, mais memória.

</details>

<details>
<summary><b>8. Rodaram o job de novo e duplicou. Como corrige e como evita?</b></summary>

Corrige removendo o lote duplicado (ou <code>RESTORE</code>). Evita com escrita idempotente: <code>MERGE</code> pela chave,
<code>replaceWhere</code> da partição ou <code>txnAppId</code>/<code>txnVersion</code> — reproduzido em §6.5.

</details>

<details>
<summary><b>9. O MERGE está lento. O que olha no `DESCRIBE HISTORY`?</b></summary>

<code>numTargetFilesRemoved</code> e <code>numTargetRowsCopied</code> (reescrita desnecessária), <code>scanTimeMs</code> ×
<code>rewriteTimeMs</code>; e, na Spark UI, os arquivos lidos antes × depois do skipping (pruning). Correção: partição/cluster na condição, deletion
vectors, dedup da origem. §6.8.

</details>

<details>
<summary><b>10. O que fazer com `ConcurrentAppendException`?</b></summary>

É controle de concorrência otimista: outra transação mudou o que esta leu. Tornar as escritas disjuntas
(partição explícita na condição), serializar escritores da mesma tabela, retry com backoff.

</details>

<details>
<summary><b>11. Como detectaria anomalia de volume em até 5 minutos sem afogar o plantão em alertas?</b></summary>

Agregação por minuto em streaming com watermark, z-score contra linha de base sazonal, volume mínimo, duas
janelas seguidas para disparar, histórico de alertas para medir falso positivo e um runbook por alerta. §4.

</details>

<details>
<summary><b>12. O custo dobrou. Onde você olha primeiro?</b></summary>

<code>system.billing.usage</code> por job/SKU/tag: cluster all-purpose ligado, streaming 24×7 sem necessidade,
autoscaling no teto por skew. Depois storage (VACUUM, retenção) e rede. Previne com cluster policies, tags e
alerta de orçamento.

</details>
