# Diagramas

> Gerado por `scripts/build_diagramas.py` a partir dos notebooks. O visualizador de notebooks do GitHub não renderiza Mermaid; esta página, sim. Não editar à mão.

## 00 · Mapa de competências e arquitetura

Notebook: [`00_mapa_de_competencias_e_arquitetura.ipynb`](../notebooks/00_mapa_de_competencias_e_arquitetura.ipynb)

<a id="nb00-1"></a>

### 3. Arquitetura alvo na Azure e a equivalente local ☁️/🧪

```mermaid
flowchart LR
    subgraph SRC["Fontes"]
        GHA["GH Archive<br/>1 arquivo JSON.gz por hora"]
        API["API REST do GitHub"]
        WM["Wikimedia EventStreams<br/>(SSE)"]
    end
    subgraph LAKE["ADLS Gen2 · Delta Lake · governado pelo Unity Catalog"]
        LAND["landing<br/>(Volume)"]
        BR["bronze<br/>bruto + linhagem"]
        SI["silver<br/>limpo, dedup, SCD2"]
        GO["gold<br/>fatos e dimensões"]
        QU["quarentena"]
    end
    GHA -- "task de download" --> LAND
    API -- "task Python<br/>token no Key Vault" --> LAND
    WM -- "Event Hubs<br/>(protocolo Kafka)" --> BR
    LAND -- "Auto Loader" --> BR
    BR -- "MERGE" --> SI
    SI -. "reprovado" .-> QU
    SI --> GO
    GO --> SQLW["SQL warehouse<br/>Power BI · Genie"]
    GO --> IA["IA: AI Functions<br/>Vector Search · Model Serving"]
    ORQ["Lakeflow Jobs<br/>bronze → silver → gold → quality"] -. "orquestra" .-> LAKE
    CICD["GitHub Actions<br/>+ Declarative Automation Bundles"] -. "deploy" .-> ORQ
    TF["Terraform<br/>workspace, ADLS, Access Connector, Key Vault"] -. "provisiona" .-> LAKE
    MON["Monitoramento<br/>system tables · alertas · Azure Monitor"] -. "observa" .-> ORQ
```

<a id="nb00-2"></a>

### 7. Lambda × Kappa 🧪

```mermaid
flowchart LR
    subgraph Lambda
        F1["fonte"] --> B["batch layer"] --> SV["serving<br/>(junta as duas)"]
        F1 --> SP["speed layer"] --> SV
    end
    subgraph Kappa
        F2["fonte"] --> LOG["log reprocessável<br/>(Event Hubs / Delta)"] --> ST["streaming"] --> OUT["tabela"]
    end
```

## 01 · Ambiente: Spark local, Databricks e Azure

Notebook: [`01_ambiente_local_databricks_azure.ipynb`](../notebooks/01_ambiente_local_databricks_azure.ipynb)

<a id="nb01-1"></a>

### 1. Driver, executor e cluster manager 🧪

```mermaid
flowchart LR
    subgraph Driver["Driver (seu código)"]
        P["plano lógico → otimizador → plano físico"] --> S["stages → tasks"]
    end
    CM["Cluster manager<br/>(local / YARN / K8s / Databricks)"]
    S -- pede recursos --> CM
    CM --> E1["Executor 1<br/>tasks + cache"]
    CM --> E2["Executor 2<br/>tasks + cache"]
    S -- envia tasks --> E1 & E2
```

<a id="nb01-2"></a>

### 6. Spark Connect (conceito) 🧪/☁️

```mermaid
flowchart LR
    C["Cliente fino<br/>(pyspark-client / databricks-connect)"] -- "plano lógico (protobuf, gRPC)" --> S["Servidor Spark Connect<br/>(driver no cluster)"]
    S -- "resultado em Arrow" --> C
    S --> X["Executors"]
```

<a id="nb01-3"></a>

### 8. Azure Databricks: workspace, rede e segredos ☁️

```mermaid
flowchart TB
    subgraph CP["Plano de controle (Databricks)"]
        UI["UI / REST API / Jobs"]
    end
    subgraph SUB["Sua assinatura Azure"]
        subgraph VNET["VNet própria (VNet injection)"]
            H["subnet host"] --- C["subnet container"]
        end
        KV["Key Vault"]
        AC["Access Connector<br/>(identidade gerenciada)"]
        ADLS["ADLS Gen2<br/>(landing, bronze, silver, gold)"]
    end
    UI -- "Secure Cluster Connectivity<br/>(sem IP público nos nós)" --> VNET
    VNET -- "Private Endpoint" --> ADLS
    VNET -- "secret scope" --> KV
    AC -- "Storage Blob Data Contributor" --> ADLS
```

## 11 · Governança, Unity Catalog e LGPD

Notebook: [`11_governanca_unity_catalog_lgpd.ipynb`](../notebooks/11_governanca_unity_catalog_lgpd.ipynb)

<a id="nb11-1"></a>

### 1. Unity Catalog: a hierarquia e o namespace de três níveis ☁️

```mermaid
flowchart TD
    M["Metastore<br/>(1 por região, por conta)"] --> C1["Catalog<br/>prod"] & C2["Catalog<br/>dev"]
    C1 --> S1["Schema<br/>bronze"] & S2["Schema<br/>silver"] & S3["Schema<br/>gold"]
    S2 --> T["Table<br/>(managed ou external)"]
    S2 --> V["View"]
    S2 --> VO["Volume<br/>(arquivos)"]
    S2 --> F["Function<br/>(UDF, máscara, filtro)"]
    S2 --> MO["Model<br/>(ML registrado)"]
    M -.-> SC["Storage credential"] -.-> EL["External location"]
```

## 13 · Git, CI/CD e Declarative Automation Bundles

Notebook: [`13_cicd_git_asset_bundles.ipynb`](../notebooks/13_cicd_git_asset_bundles.ipynb)

<a id="nb13-1"></a>

### 6. Pirâmide de testes para dados 🧪

```mermaid
flowchart TB
    E2E["Ponta a ponta<br/>job inteiro no alvo dev do bundle; make demo"] --> DQ
    DQ["Testes de dados / contrato<br/>expectations, schema, frescor, volume — rodam em produção, a cada lote"] --> INT
    INT["Integração<br/>Spark local + Delta: ingestão idempotente, MERGE"] --> UNIT
    UNIT["Unitários<br/>funções puras e transformações pequenas (chispa)"]
```

<a id="nb13-2"></a>

### 8. Declarative Automation Bundles: o job como código 🧪/☁️

```mermaid
flowchart LR
    B["bronze<br/>retries 2 · 30 min"] --> S["silver<br/>retries 2 · 30 min"] --> G["gold<br/>retries 1 · 30 min"] --> Q["quality<br/>retries 0 · 15 min"]
```

<a id="nb13-3"></a>

### 9. GitHub Actions: CI e deploy por ambiente 🧪/☁️

```mermaid
flowchart LR
    PR["PR / push"] --> L["lint"] & T["test<br/>Java 17 + Spark"] & TF["terraform<br/>fmt + validate"]
    L & T --> BV["bundle validate<br/>(só com host configurado)"]
    L & T & TF & BV --> DD{"push na main?"} -->|sim| DEV["deploy dev"]
    L & T & TF & BV --> DP{"tag v*?"} -->|sim| APR["aprovação<br/>ambiente prod"] --> PROD["deploy prod"]
```

## 17 · Simulado de system design e troubleshooting

Notebook: [`17_simulado_system_design.ipynb`](../notebooks/17_simulado_system_design.ipynb)

<a id="nb17-1"></a>

### 2. Caso (a): ingestão de 1 TB/dia de eventos de app na Azure com Databricks

```mermaid
flowchart LR
  app[Apps web/mobile] -->|HTTPS + SDK| col[API de coleta<br/>App Service / APIM]
  col --> eh[(Event Hubs<br/>protocolo Kafka)]
  eh -->|Structured Streaming<br/>ou Lakeflow| bz[(Bronze Delta<br/>evento bruto + metadados)]
  eh -.->|Capture: Avro no ADLS<br/>replay barato| raw[(ADLS Gen2<br/>landing)]
  bz --> sv[(Silver<br/>tipado, dedup, PII tratada)]
  sv --> gd[(Gold<br/>fatos e métricas)]
  gd --> bi[Databricks SQL / Power BI]
  sv --> ml[Feature tables / ML]
  uc{{Unity Catalog}} -.->|governa| bz & sv & gd
  kv{{Key Vault}} -.->|segredos| eh
```

<a id="nb17-2"></a>

### 3. Caso (b): CDC de um banco transacional para o lakehouse

```mermaid
flowchart LR
  db[(SQL Server / Postgres<br/>log de transações)] -->|CDC| cap{Debezium · Lakeflow Connect · ADF}
  cap --> raw[(Bronze: log de mudanças<br/>op, chave, colunas, LSN)]
  raw -->|dedup por chave mantendo o maior LSN| m[MERGE]
  m --> cur[(Silver: estado atual<br/>SCD1 ou SCD2)]
  cur --> gold[(Gold)]
```

<a id="nb17-3"></a>

### 4. Caso (c): near-real-time para detecção de anomalia (SLA de minutos)

```mermaid
flowchart LR
  eh[(Event Hubs)] --> ss[Structured Streaming<br/>janela de 1 min + watermark]
  ss --> agg[(Delta: contagem por minuto<br/>e dimensão)]
  agg --> det[Detecção<br/>z-score vs. linha de base]
  base[(Linha de base<br/>gold: média/desvio por hora e dia da semana)] --> det
  det -->|anomalia| al[Alerta: Teams / e-mail<br/>via SQL Alert ou Logic App]
  det --> hist[(Histórico de alertas<br/>para medir falso positivo)]
```

<a id="nb17-4"></a>

### 5. Caso (d): migração de um DW legado (SQL Server / Synapse) para o Databricks

```mermaid
flowchart LR
  src[Fontes: ERP, CRM, arquivos] --> leg[(DW legado<br/>SQL Server / Synapse)]
  src --> bz[(Bronze)] --> sv[(Silver)] --> gd[(Gold)]
  leg -.->|Lakehouse Federation<br/>na transição| gd
  leg --> rec{Reconciliação diária<br/>contagem · checksum · diff por chave}
  gd --> rec
  rec -->|paridade ok N dias| cut[Corte por relatório<br/>Power BI aponta para o Databricks SQL]
```
