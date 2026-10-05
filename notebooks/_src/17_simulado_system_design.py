# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 17 · Simulado de system design e troubleshooting
#
# > Prova que dá para responder uma pergunta aberta de arquitetura de dados com método — requisitos, conta de
# > guardanapo, desenho, decisões e trade-offs — e diagnosticar os 10 incidentes clássicos de Spark/Delta com
# > sintoma, causa e correção, reproduzindo localmente os que cabem em segundos.
#
# | Requisito da vaga | Onde aparece aqui |
# |---|---|
# | Arquitetura e desenvolvimento de pipelines | §1 (framework), §2–§5 (4 casos) |
# | Microsoft Azure | Event Hubs, ADLS Gen2, ADF, Key Vault, Entra ID, Monitor nos 4 casos |
# | Databricks e processamento de dados | Auto Loader, Structured Streaming, Lakeflow, MERGE, Unity Catalog; §6 runbook |
# | IA aplicada à engenharia de dados | §4 (detecção de anomalia), §5 (conversão de SQL legado assistida) |
# | Times ágeis multidisciplinares | §1 (requisitos com o negócio), §7 (perguntas ao entrevistador) |
#
# Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)
#
# **Como usar:** leia o framework (§1), tente responder cada caso em voz alta em 10–15 min, e só depois abra o
# desenho e as decisões. O runbook (§6) é para a pergunta "conte de um incidente que você resolveu".

# %% [markdown]
# ## Setup

# %%
import gzip
import shutil
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from oss_lakehouse.bronze import GH_EVENT_SCHEMA
from oss_lakehouse.config import get_settings
from oss_lakehouse.spark import get_spark
from oss_lakehouse.utils.io import count_lines_gz

spark = get_spark("17")
s = get_settings()
LANDING = Path(s.path("landing", "gharchive"))
DEMO = Path(s.data_root) / "demo" / "17"
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True)
bronze = spark.read.format("delta").load(s.path("bronze", "gh_events"))
print(f"bronze: {bronze.count():,} eventos | demo em {DEMO.relative_to(Path(s.data_root).parent)}")

# %% [markdown]
# ## 1. O framework de resposta 🧪
#
# **O que é** — um roteiro fixo para perguntas abertas ("desenhe um pipeline que..."). O entrevistador não
# avalia se você acertou "a" arquitetura; avalia **se você pergunta antes de desenhar, se faz conta, se nomeia
# trade-offs e se pensa em operação** (falha, custo, segurança).
#
# **Por que importa** — o erro clássico é desenhar caixas nos primeiros 30 segundos. Sem requisito, toda
# escolha é chute; sem volumetria, "Kafka" ou "Spark" é moda, não decisão.
#
# **Como funciona** — 10 passos, com o tempo aproximado numa entrevista de 45 min:
#
# | # | Passo | Perguntas / o que entregar | ~min |
# |---|---|---|---|
# | 1 | **Requisitos funcionais** | Quem consome? Que pergunta responde? Grão? Histórico (SCD)? | 4 |
# | 2 | **Não funcionais** | Latência (SLA: segundos, minutos, D+1?), volume, crescimento, disponibilidade, retenção, custo-alvo | 3 |
# | 3 | **Volumetria de guardanapo** | eventos/s média e pico, MB/s, TB armazenados, nº de arquivos, núcleos | 4 |
# | 4 | **Arquitetura** | Diagrama: fonte → ingestão → bronze/silver/gold → consumo; batch × streaming | 8 |
# | 5 | **Modelagem** | Chaves, partição/clustering, SCD, fato × dimensão, contrato | 5 |
# | 6 | **Confiabilidade e reprocessamento** | Idempotência, checkpoint, backfill, dado atrasado, *exactly-once* | 5 |
# | 7 | **Qualidade** | Expectations, quarentena, reconciliação com a fonte, frescor (*freshness*) | 3 |
# | 8 | **Segurança / LGPD** | PII, mascaramento, grants, segredos, retenção e direito ao esquecimento | 3 |
# | 9 | **Observabilidade** | Métricas do pipeline, atraso, alertas, linhagem, quem é acionado | 3 |
# | 10 | **Custo e evolução** | O que domina o custo, como escalar 10×, o que fica para a v2 | 4 |
#
# A conta de guardanapo (*back-of-the-envelope*) começa por **calibrar com dado real**. Medimos o GH Archive:
# bytes por evento em JSON, a compressão do gzip e do Delta (Parquet), e a vazão de parse do Spark por núcleo.

# %%
arqs = sorted(LANDING.glob("*.json.gz"))
gz_bytes = sum(a.stat().st_size for a in arqs)
json_bytes = 0
for a in arqs:
    with gzip.open(a, "rb") as f:
        while chunk := f.read(1 << 22):
            json_bytes += len(chunk)
n_eventos = sum(count_lines_gz(a) for a in arqs)
delta_bytes = sum(p.stat().st_size for p in Path(s.path("bronze", "gh_events")).rglob("*.parquet"))

t0 = time.perf_counter()
spark.read.schema(GH_EVENT_SCHEMA).json(str(LANDING)).write.format("noop").mode("overwrite").save()
t_parse = time.perf_counter() - t0
tarefas = len(arqs)  # .gz não é divisível: 1 arquivo = 1 tarefa = 1 núcleo

CAL = {
    "bytes_por_evento_json": json_bytes / n_eventos,
    "compressao_gzip": json_bytes / gz_bytes,
    "compressao_delta": json_bytes / delta_bytes,
    "mb_s_por_nucleo": json_bytes / 1e6 / t_parse / tarefas,
}
print(f"{n_eventos:,} eventos | JSON {json_bytes / 1e6:.0f} MB | gzip {gz_bytes / 1e6:.0f} MB | "
      f"Delta {delta_bytes / 1e6:.0f} MB")
for k, v in CAL.items():
    print(f"  {k:22} {v:8.1f}")

# %% [markdown]
# Com a calibração, a calculadora. Toda premissa é parâmetro explícito — em entrevista, **diga a premissa em voz
# alta** antes do número.

# %%
def volumetria(
    gb_por_dia: float,
    bytes_por_evento: float,
    pico_sobre_media: float = 4.0,
    compressao: float = CAL["compressao_delta"],
    retencao_dias: int = 365,
    mb_s_por_nucleo: float = CAL["mb_s_por_nucleo"],
    folga: float = 2.0,
    arquivo_alvo_mb: int = 256,
) -> dict[str, float]:
    """Conta de guardanapo de um pipeline de eventos. Devolve as grandezas que orientam o desenho."""
    eventos_dia = gb_por_dia * 1e9 / bytes_por_evento
    media_s = eventos_dia / 86_400
    pico_s = media_s * pico_sobre_media
    mb_s_pico = pico_s * bytes_por_evento / 1e6
    armazenado_dia_gb = gb_por_dia / compressao
    return {
        "eventos/dia (milhões)": eventos_dia / 1e6,
        "eventos/s média": media_s,
        "eventos/s pico": pico_s,
        "MB/s pico (bruto)": mb_s_pico,
        "GB/dia no Delta": armazenado_dia_gb,
        f"TB em {retencao_dias} dias": armazenado_dia_gb * retencao_dias / 1e3,
        f"arquivos/dia de {arquivo_alvo_mb} MB": armazenado_dia_gb * 1e3 / arquivo_alvo_mb,
        "núcleos p/ o pico (c/ folga)": mb_s_pico / mb_s_por_nucleo * folga,
        "núcleos p/ o dia em 1 h (batch)": gb_por_dia * 1e3 / 3_600 / mb_s_por_nucleo * folga,
    }


def mostra(v: dict[str, float]) -> None:
    for k, x in v.items():
        print(f"  {k:34} {x:>14,.1f}")


print("Sanidade — o próprio GH Archive (~1 dia = 24 × a hora medida):")
mostra(volumetria(json_bytes / len(arqs) * 24 / 1e9, CAL["bytes_por_evento_json"], retencao_dias=30))

# %% [markdown]
# > 🎤 **Resposta de 30 s (sobre o método):** "Antes de desenhar, pergunto quem consome, qual a latência
# > aceitável e o volume. Faço a conta: eventos por segundo na média e no pico, MB/s, quanto ocupa comprimido,
# > quantos núcleos. Aí escolho batch ou streaming, desenho bronze/silver/gold, e fecho com idempotência,
# > qualidade, segurança, observabilidade e custo — dizendo o trade-off de cada escolha."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Números de bolso:** 1 dia = 86.400 s (~10⁵); 1 milhão/dia ≈ 12/s; 1 bilhão/dia ≈ 12 mil/s. Pico típico
#   2–5× a média (eventos de app seguem o horário comercial).
# - **A calibração acima é de um laptop** com 1 núcleo por arquivo `.gz`: serve como ordem de grandeza, não
#   como sizing final. Em produção se mede com um piloto e se ajusta o cluster (ou se usa serverless).
# - **Compressão** depende do dado: JSON repetitivo comprime muito; Parquet colunar comprime mais quando a
#   coluna tem baixa cardinalidade. Aqui o `payload` é uma string JSON por linha — por isso o Delta comprimiu
#   **menos** que o gzip do arquivo inteiro (números acima). Com o payload tipado em colunas, a relação inverte.
# </details>
#
# **Trade-offs / quando NÃO usar**
# - O framework é um roteiro, não um formulário: se o entrevistador quer ir direto para o MERGE, vá.
# - Volumetria com precisão falsa ("37,4 núcleos") soa mal; arredonde e diga a ordem de grandeza.

# %% [markdown]
# ## 2. Caso (a): ingestão de 1 TB/dia de eventos de app na Azure com Databricks
#
# **Enunciado.** "Nosso app (web e mobile) gera eventos de uso: telas, cliques, compras. Hoje são 1 TB/dia de
# JSON. Queremos um lakehouse na Azure com Databricks para produto e marketing analisarem, e o time de dados
# treinar modelos. Desenhe."
#
# **1–2. Requisitos — o que perguntar:**
# - Latência: o dashboard de produto aceita D+1 ou precisa de minutos? (muda batch × streaming e o custo)
# - Quem envia: SDK próprio, Segment/RudderStack, App Insights? Há garantia de ordem ou de entrega única?
# - Formato estável? Quem é dono do schema do evento (contrato)? Qual taxa de mudança?
# - PII: e-mail, device id, localização? Base legal LGPD, retenção, direito ao esquecimento?
# - Crescimento esperado (2× ao ano?) e pico (Black Friday?).
#
# **3. Volumetria** — premissas: 1 TB/dia de JSON, evento médio de 1 KB (menor que o do GitHub), pico 4× a
# média, retenção de 2 anos na silver; compressão e vazão calibradas acima.

# %%
caso_a = volumetria(gb_por_dia=1_000, bytes_por_evento=1_000, pico_sobre_media=4, retencao_dias=730)
mostra(caso_a)
print(f"\nEvent Hubs: 1 unidade de vazão (TU, Standard) ≈ 1 MB/s ou 1.000 eventos/s de entrada →"
      f" ~{max(caso_a['MB/s pico (bruto)'], caso_a['eventos/s pico'] / 1000):.0f} TU no pico")

# %% [markdown]
# **Leitura:** o pico pede dezenas de TU — na casa do teto de um namespace Standard (40 TU quando este material foi
# escrito; confira o limite atual na documentação da Azure). Ou seja: o desenho já nasce no tier Premium/Dedicated
# ou com mais de um namespace. Em armazenamento, centenas de TB em 2 anos: o custo de storage importa, e a
# política de retenção da bronze também.
#
# **4. Arquitetura**
#
# ```mermaid
# flowchart LR
#   app[Apps web/mobile] -->|HTTPS + SDK| col[API de coleta<br/>App Service / APIM]
#   col --> eh[(Event Hubs<br/>protocolo Kafka)]
#   eh -->|Structured Streaming<br/>ou Lakeflow| bz[(Bronze Delta<br/>evento bruto + metadados)]
#   eh -.->|Capture: Avro no ADLS<br/>replay barato| raw[(ADLS Gen2<br/>landing)]
#   bz --> sv[(Silver<br/>tipado, dedup, PII tratada)]
#   sv --> gd[(Gold<br/>fatos e métricas)]
#   gd --> bi[Databricks SQL / Power BI]
#   sv --> ml[Feature tables / ML]
#   uc{{Unity Catalog}} -.->|governa| bz & sv & gd
#   kv{{Key Vault}} -.->|segredos| eh
# ```
#
# **5. Modelagem:** bronze com o envelope estável tipado e o corpo do evento como `VARIANT` (ou string JSON);
# silver com uma tabela por família de evento, deduplicada por `event_id`; gold com fatos de sessão/compra e
# dimensões de usuário (SCD2) e produto. Liquid Clustering por `event_date, event_name` em vez de partição
# física de alta cardinalidade.
#
# **6. Confiabilidade:** o checkpoint do streaming garante cada offset processado uma vez; o *sink* Delta é
# transacional → *exactly-once* fim a fim **dentro** do lakehouse. Duplicata do próprio app (retry do SDK) se
# resolve na silver por `event_id`. Reprocessamento: o Event Hubs retém dias, o Capture no ADLS retém meses —
# replay sem depender do app.
#
# **7–9. Qualidade, segurança, observabilidade:** expectations na silver (evento sem `user_id`, timestamp no
# futuro) com quarentena; PII hasheada/tokenizada na silver e *column mask* no Unity Catalog; identidade gerenciada
# (Access Connector) para o ADLS, sem chave em código; alerta de atraso do consumidor e de frescor da gold.
#
# **10. Custo e evolução:** streaming contínuo 24×7 custa cluster ligado; se o SLA for de horas, `trigger(availableNow)`
# agendado a cada 15–60 min dá o mesmo código pela fração do custo.
#
# | Decisão | Escolha | Alternativa | Por quê |
# |---|---|---|---|
# | Barramento | Event Hubs (Kafka API) | Kafka gerenciado (Confluent), IoT Hub | Nativo Azure, cliente Kafka sem mudar código, Capture embutido |
# | Ingestão no lake | Structured Streaming / Lakeflow | Event Hubs Capture + Auto Loader | Latência de minutos; Capture + Auto Loader é mais barato se D+1 bastar |
# | Formato do evento | `VARIANT` na bronze | schema fixo | Evento muda toda sprint; schema fixo quebra a ingestão |
# | Layout | Liquid Clustering | `PARTITIONED BY (date, event)` | Evita small files em partições de cauda longa |
# | Disparo | `availableNow` a cada 15 min | contínuo | ~mesma entrega para SLA de 30 min, cluster desligado entre execuções |
#
# | Um pleno responderia | Um sênior responde |
# |---|---|
# | "Kafka → Spark → Delta → Power BI." | Pergunta SLA e volume antes; faz a conta (eventos/s, TU, TB); justifica streaming × batch pelo custo |
# | Partição por data e tipo de evento | Mostra o risco de small files e propõe Liquid Clustering; fala de `OPTIMIZE` e tamanho-alvo |
# | "O Spark garante exactly-once" | Explica onde (offset + checkpoint + sink transacional) e onde **não** (retry do app) → dedup por `event_id` |
# | Não menciona PII | Tokeniza na silver, mascara no UC, define retenção e como atender pedido de exclusão (LGPD) |
#
# > 🎤 **Resposta de 30 s:** "1 TB/dia de 1 KB são ~1 bilhão de eventos, ~12 mil/s na média e ~50 mil no pico.
# > App → API de coleta → Event Hubs com protocolo Kafka → Structured Streaming no Databricks gravando a bronze
# > em Delta, com Capture no ADLS para replay barato. Silver deduplica por `event_id`, tipa e trata PII; gold tem
# > fatos e métricas para o Databricks SQL. Se o SLA for de 30 minutos, rodo `availableNow` agendado em vez de
# > streaming contínuo: mesmo código, bem menos custo."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Partições do Event Hubs** definem o paralelismo máximo do consumidor (1 tarefa por partição); dimensione
#   pelo pico com folga — aumentar depois depende do tier.
# - **Ordenação** só é garantida dentro da partição: a chave de partição (`user_id`) mantém a ordem por usuário.
# - **Dado atrasado (mobile offline):** evento chega horas depois. Watermark na agregação de streaming e
#   gold recalculada por janela de dias (MERGE) — notebook 06.
# - **Backfill de 1 ano:** do Capture/ADLS com Auto Loader em batch, cluster dedicado, sem disputar com o
#   streaming.
# </details>

# %% [markdown]
# ## 3. Caso (b): CDC de um banco transacional para o lakehouse
#
# **Enunciado.** "O ERP roda em SQL Server (ou Postgres). Queremos as tabelas de pedidos e clientes no lakehouse
# com atraso de no máximo 15 minutos, incluindo deletes, sem pesar no banco de produção."
#
# **1–2. Requisitos — o que perguntar:** quais tabelas e tamanho; taxa de mudança (linhas/min) e pico; o
# banco já tem CDC/replicação lógica habilitado? (precisa de DBA); os deletes são físicos ou lógicos? precisa de
# histórico (SCD2) ou só o estado atual? há chave primária em todas as tabelas? janela de manutenção?
#
# **Opções de captura** — *CDC (Change Data Capture)* lê o log de transações do banco (não consulta as tabelas):
#
# | Opção | Como funciona | Bom quando | Cuidado |
# |---|---|---|---|
# | **Debezium** (Kafka Connect) | Lê o log (SQL Server CDC / Postgres replicação lógica) e publica no Kafka/Event Hubs | Muitos consumidores além do lake; latência de segundos | Operar Kafka Connect; *snapshot* inicial; schema history |
# | **Lakeflow Connect** (Databricks) | Conector gerenciado que lê o CDC e grava Delta direto | Querer menos peças para operar | Bancos e recursos suportados variam — confira a doc atual |
# | **ADF** (Azure Data Factory) | Cópia incremental por marca d'água (`updated_at`) ou pelo recurso de CDC do ADF | Lote a cada 15+ min, time já usa ADF | Marca d'água **não vê delete físico**; carga no banco |
#
# ```mermaid
# flowchart LR
#   db[(SQL Server / Postgres<br/>log de transações)] -->|CDC| cap{Debezium · Lakeflow Connect · ADF}
#   cap --> raw[(Bronze: log de mudanças<br/>op, chave, colunas, LSN)]
#   raw -->|dedup por chave mantendo o maior LSN| m[MERGE]
#   m --> cur[(Silver: estado atual<br/>SCD1 ou SCD2)]
#   cur --> gold[(Gold)]
# ```
#
# **O coração do caso: aplicar o log com MERGE.** Três armadilhas, reproduzidas abaixo: (1) o lote tem **duas
# mudanças da mesma chave** — o MERGE falha; (2) mudança **fora de ordem** (LSN menor que o já aplicado) —
# sobrescreve dado novo com velho; (3) **delete** — precisa virar `DELETE` (ou *soft delete*).
#
# *LSN* (*Log Sequence Number*) é a posição da mudança no log do banco: a ordem verdadeira, melhor que timestamp
# (dois updates no mesmo milissegundo têm LSNs diferentes).

# %%
alvo = str(DEMO / "clientes")
spark.createDataFrame(
    [(1, "ana", "free", 10), (2, "bia", "pro", 11), (3, "caio", "free", 12)],
    "id int, nome string, plano string, lsn long",
).write.format("delta").save(alvo)

lote_cdc = spark.createDataFrame(
    [
        ("u", 1, "ana", "pro", 20),
        ("u", 1, "ana", "enterprise", 25),  # 2 mudanças da mesma chave no lote
        ("d", 2, None, None, 21),  # delete
        ("i", 4, "duda", "free", 22),  # insert
        ("u", 3, "caio", "pro", 5),  # atrasada: LSN 5 < 12 já aplicado → ignorar
    ],
    "op string, id int, nome string, plano string, lsn long",
)

tabela = DeltaTable.forPath(spark, alvo)
try:
    tabela.alias("t").merge(lote_cdc.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
except Exception as exc:
    print("MERGE ingênuo:", str(exc).split("]")[0] + "]")

# %%
def aplicar_cdc(lote):
    """Aplica um lote de CDC: última mudança por chave (maior LSN), ignora atrasadas, trata delete."""
    w = Window.partitionBy("id").orderBy(F.col("lsn").desc())
    ultima = lote.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
    (
        DeltaTable.forPath(spark, alvo).alias("t")
        .merge(ultima.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.op = 'd' AND s.lsn > t.lsn")
        .whenMatchedUpdate(condition="s.op <> 'd' AND s.lsn > t.lsn",
                           set={"nome": "s.nome", "plano": "s.plano", "lsn": "s.lsn"})
        .whenNotMatchedInsert(condition="s.op <> 'd'",
                              values={"id": "s.id", "nome": "s.nome", "plano": "s.plano", "lsn": "s.lsn"})
        .execute()
    )


def estado():
    return sorted(tuple(r) for r in spark.read.format("delta").load(alvo).collect())


aplicar_cdc(lote_cdc)
esperado = [(1, "ana", "enterprise", 25), (3, "caio", "free", 12), (4, "duda", "free", 22)]
assert estado() == esperado, estado()
aplicar_cdc(lote_cdc)  # reprocessar o mesmo lote (retry do job) não muda nada: idempotente
assert estado() == esperado
print("estado após o lote (e após reaplicá-lo):", estado())

# armadilha do delete físico: um update ATRASADO da chave 2 (LSN 15 < delete 21) chega depois
aplicar_cdc(spark.createDataFrame([("u", 2, "bia", "free", 15)], lote_cdc.schema))
print("update atrasado de uma chave apagada:", [r for r in estado() if r[0] == 2], "← ressuscitou!")

# %% [markdown]
# A chave 2 voltou: com delete **físico**, o MERGE perdeu o LSN do delete e não tem com o que comparar. A
# correção é **soft delete** (manter a linha com `is_deleted = true` e o LSN) na silver e filtrar na leitura —
# ou guardar os *tombstones* por um tempo. No Lakeflow Spark Declarative Pipelines, `AUTO CDC ... SEQUENCE BY lsn`
# (antigo `APPLY CHANGES INTO`) faz exatamente isso: dedup, ordem, delete e SCD1/SCD2, com tombstones.
#
# | Um pleno responderia | Um sênior responde |
# |---|---|
# | "Uso o ADF com `updated_at`." | Pergunta sobre deletes físicos: marca d'água não os vê → CDC pelo log |
# | `MERGE ... WHEN MATCHED UPDATE` | Dedup por chave com maior LSN, condição `s.lsn > t.lsn`, `DELETE`, idempotência provada |
# | Ignora o snapshot inicial | Carga inicial consistente (snapshot + LSN de corte) e só depois o fluxo contínuo |
# | — | Delete físico + evento atrasado ressuscita a linha → soft delete/tombstone |
# | — | Mudança de schema no banco (coluna nova) → schema evolution controlada no MERGE, alerta ao dono |
#
# > 🎤 **Resposta de 30 s:** "CDC lendo o log de transações — Debezium no Event Hubs se há outros consumidores,
# > ou Lakeflow Connect se quero menos peças. A bronze guarda o log de mudanças cru; a silver aplica com MERGE:
# > última mudança por chave pelo LSN, só aplica se o LSN for maior que o da tabela, delete vira delete ou soft
# > delete. Reprocessar o mesmo lote não muda nada. Marca d'água por `updated_at` eu evito: não enxerga delete."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Carga no banco:** CDC lê o log, não as tabelas; o snapshot inicial sim — faça em réplica de leitura ou na
#   janela de manutenção.
# - **Retenção do log:** se o pipeline parar mais que a retenção do CDC do SQL Server (padrão de dias) ou o
#   *replication slot* do Postgres acumular WAL, o banco sofre (disco) ou o pipeline perde mudanças → alerta de
#   atraso do consumidor é obrigatório.
# - **Transações:** o MERGE por lote pode aplicar metade de uma transação de várias tabelas (pedido sem itens);
#   se a consistência entre tabelas importa, processe por LSN de commit e publique a gold só até o último commit
#   completo.
# - **MERGE em tabela grande:** ver o runbook "MERGE lento" (§6.8) — pruning e deletion vectors.
# </details>

# %% [markdown]
# ## 4. Caso (c): near-real-time para detecção de anomalia (SLA de minutos)
#
# **Enunciado.** "Queremos ser avisados em até 5 minutos quando o volume de algum tipo de evento sair do padrão
# (queda de pagamentos, pico de erros)."
#
# **1–2. Requisitos — o que perguntar:** o que é "anomalia" (queda, pico, ambos)? por qual dimensão (tipo,
# região, app)? quantos falsos positivos o time tolera por dia? quem recebe o alerta e o que faz com ele
# (runbook)? Precisão importa mais que latência, ou o contrário?
#
# ```mermaid
# flowchart LR
#   eh[(Event Hubs)] --> ss[Structured Streaming<br/>janela de 1 min + watermark]
#   ss --> agg[(Delta: contagem por minuto<br/>e dimensão)]
#   agg --> det[Detecção<br/>z-score vs. linha de base]
#   base[(Linha de base<br/>gold: média/desvio por hora e dia da semana)] --> det
#   det -->|anomalia| al[Alerta: Teams / e-mail<br/>via SQL Alert ou Logic App]
#   det --> hist[(Histórico de alertas<br/>para medir falso positivo)]
# ```
#
# **Mecanismo:** streaming agrega por janela de 1 minuto (com *watermark* — o limite de atraso aceito para um
# evento entrar na janela certa) e compara com a linha de base. A detecção mais simples e explicável é o
# **z-score**: quantos desvios-padrão o valor está da média recente. A mesma lógica, em batch, sobre a bronze:

# %%
por_min = (
    bronze.withColumn("minuto", F.date_trunc("minute", F.to_timestamp("created_at")))
    .groupBy("type", "minuto").count()
)
w = Window.partitionBy("type").orderBy("minuto").rowsBetween(-30, -1)  # 30 min anteriores, sem o atual
z = (
    por_min.withColumn("media", F.avg("count").over(w))
    .withColumn("desvio", F.stddev("count").over(w))
    .withColumn("z", (F.col("count") - F.col("media")) / F.col("desvio"))
    .where(F.col("desvio") > 0)
)
anomalias = z.where(F.abs("z") > 4)
print(f"minutos avaliados: {z.count():,} | anomalias (|z| > 4): {anomalias.count()}")
anomalias.select(
    "type", F.date_format("minuto", "HH:mm").alias("min_utc"), "count", F.round("media", 1).alias("media_30min"),
    F.round("z", 1).alias("z"),
).orderBy(F.desc(F.abs("z"))).show(6, truncate=False)

# %% [markdown]
# Leitura: com **3 horas** de dado, a linha de base de 30 min é curta e tipos de baixo volume (poucos eventos
# por minuto) geram z alto por acaso — exatamente o falso positivo que o requisito precisa limitar (volume
# mínimo, linha de base por hora do dia/dia da semana, duas janelas seguidas para disparar).
#
# | Decisão | Escolha | Alternativa | Por quê |
# |---|---|---|---|
# | Motor | Structured Streaming (micro-lote de 1 min) | Real-time mode do Databricks, Stream Analytics, Azure Data Explorer | SLA de minutos não pede sub-segundo; reaproveita o lakehouse |
# | Detecção | z-score / EWMA com linha de base sazonal | Modelo de ML (Isolation Forest, Prophet) | Explicável, barato; ML quando as regras não bastarem e houver rótulo |
# | Alerta | Tabela de alertas + Databricks SQL Alert / Logic App | E-mail direto do job | Histórico permite medir falso positivo e silenciar |
#
# | Um pleno responderia | Um sênior responde |
# |---|---|
# | "Streaming com Spark e um threshold fixo." | Linha de base sazonal, volume mínimo, histerese (2 janelas) — e mede falso positivo |
# | Esquece dado atrasado | Watermark: quanto esperar × latência do alerta; evento atrasado corrige a métrica, não re-alerta |
# | Cluster de streaming 24×7 sem pensar | Avalia custo: micro-lote a cada 1–2 min com cluster pequeno, ou serverless |
# | — | Define o dono do alerta e o runbook: alerta sem ação vira ruído e é ignorado |
#
# > 🎤 **Resposta de 30 s:** "Event Hubs → Structured Streaming agregando por minuto com watermark → tabela Delta
# > de métricas → detecção por z-score contra uma linha de base sazonal da gold → alerta com histórico. Começo
# > explicável e meço falso positivo; ML só quando a regra não der conta. 5 minutos de SLA cabem em micro-lote —
# > não pago sub-segundo sem precisar."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Estado do streaming:** janelas com watermark limpam o estado antigo; sem watermark, o estado cresce até
#   o OOM. RocksDB como *state store* para estado grande (notebook 06).
# - **IA aplicada:** um LLM pode **resumir** o alerta (o que caiu, desde quando, quais dimensões) para quem está
#   de plantão — não decidir se é anomalia (notebook 12).
# - **Detecção de "nada chegou"**: z-score não dispara se não há linha; monitore frescor (último evento recebido)
#   separadamente.
# </details>

# %% [markdown]
# ## 5. Caso (d): migração de um DW legado (SQL Server / Synapse) para o Databricks
#
# **Enunciado.** "Temos um DW em SQL Server (ou Synapse dedicated pool) com 400 tabelas, 2.000 procedures e
# relatórios Power BI. Queremos migrar para o Databricks sem parar o negócio."
#
# **1–2. Requisitos — o que perguntar:** por que migrar (custo, escala, IA, fim de suporte)? quais relatórios são
# críticos e quem assina a paridade? existe janela de *freeze* de mudanças? o legado continua recebendo dado
# durante a migração? prazo e orçamento? o que **não** migra (relatório morto)?
#
# **Estratégia (em ondas, não *big bang*):**
# 1. **Inventário e linhagem:** quem lê o quê (logs de consulta do SQL Server/Synapse), o que está morto,
#    dependências entre procedures. Prioridade = valor × facilidade.
# 2. **Fundação:** Unity Catalog, ambientes, CI/CD, padrões (bronze/silver/gold) — antes da 1ª tabela.
# 3. **Ondas por domínio:** ingerir as fontes originais (não o DW!) na bronze; reescrever as transformações;
#    *Lakehouse Federation* para consultar o legado durante a transição.
# 4. **Paridade:** rodar os dois em paralelo (*dual run*) e **reconciliar** todo dia.
# 5. **Corte:** por consumidor (relatório a relatório), com plano de volta (*rollback*) e data de desligamento.
#
# ```mermaid
# flowchart LR
#   src[Fontes: ERP, CRM, arquivos] --> leg[(DW legado<br/>SQL Server / Synapse)]
#   src --> bz[(Bronze)] --> sv[(Silver)] --> gd[(Gold)]
#   leg -.->|Lakehouse Federation<br/>na transição| gd
#   leg --> rec{Reconciliação diária<br/>contagem · checksum · diff por chave}
#   gd --> rec
#   rec -->|paridade ok N dias| cut[Corte por relatório<br/>Power BI aponta para o Databricks SQL]
# ```
#
# **A reconciliação, reproduzida.** O "legado" é uma amostra da bronze; o "novo" é a mesma amostra com 3 linhas
# faltando, 2 com tipo diferente e 1 duplicada — os defeitos clássicos de uma reescrita. A reconciliação vai em
# níveis: contagem por partição (barato) → checksum por partição → diff por chave (caro, só onde divergiu).

# %%
cols = ["id", "type", "actor_id", "repo_id", "created_at"]
legado = bronze.select("id", "type", F.col("actor.id").alias("actor_id"), F.col("repo.id").alias("repo_id"),
                       "created_at").where(F.col("id").substr(-2, 2) == "07")  # amostra determinística ~1%
ids = [r.id for r in legado.orderBy("id").limit(6).collect()]
novo = (
    legado.where(~F.col("id").isin(ids[:3]))  # 3 faltando
    .withColumn("type", F.when(F.col("id").isin(ids[3:5]), F.lit("PushEvent_v2")).otherwise(F.col("type")))
    .unionByName(legado.where(F.col("id") == ids[5]))  # 1 duplicada
)
legado, novo = legado.cache(), novo.cache()


def reconciliar(a, b, chave: str, colunas: list[str], particao):
    """Nível 1: contagem e checksum por partição. Nível 2: diff por chave só nas partições divergentes."""
    def resumo(df):
        return df.groupBy(particao.alias("p")).agg(
            F.count("*").alias("linhas"), F.sum(F.xxhash64(*colunas).cast("decimal(38,0)")).alias("checksum"))  # decimal: sem overflow
    r = resumo(a).alias("a").join(resumo(b).alias("b"), "p", "full")
    divergentes = r.where("a.linhas <> b.linhas OR a.checksum <> b.checksum OR a.linhas IS NULL OR b.linhas IS NULL")
    print(f"nível 1 — partições: {r.count()}, divergentes: {divergentes.count()}")
    ps = [x.p for x in divergentes.select("p").collect()]
    a2, b2 = a.where(particao.isin(ps)), b.where(particao.isin(ps))
    faltando = a2.join(b2, chave, "left_anti").count()
    sobrando = b2.join(a2, chave, "left_anti").count()
    dup = b2.groupBy(chave).count().where("count > 1").count()
    h = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in colunas]), 256)
    diff = (a2.withColumn("h", h).alias("a").join(b2.dropDuplicates([chave]).withColumn("h", h).alias("b"), chave)
            .where("a.h <> b.h"))
    print(f"nível 2 — faltando no novo: {faltando} | sobrando: {sobrando} | chaves duplicadas: {dup} | "
          f"valores divergentes: {diff.count()}")
    return diff.select(chave, F.col("a.type").alias("type_legado"), F.col("b.type").alias("type_novo"))


hora = F.substring("created_at", 12, 2)  # partição de reconciliação: hora UTC
print(f"legado: {legado.count():,} linhas | novo: {novo.count():,} linhas")
reconciliar(legado, novo, "id", cols, hora).show(truncate=False)

# %% [markdown]
# Note que a **contagem total** quase bate (3 faltando e 1 duplicada dão diferença líquida de 2): reconciliar só
# por `COUNT(*)` esconderia defeitos que se compensam — e os 2 valores trocados nem aparecem na contagem. O checksum por partição (`xxhash64` somado — independente da ordem) aponta onde
# olhar; o diff por chave diz o quê.
#
# | Um pleno responderia | Um sênior responde |
# |---|---|
# | "Converto as procedures para PySpark e migro tudo." | Inventário + linhagem, desliga o que está morto, migra em ondas por domínio com corte por consumidor |
# | "Comparo a contagem de linhas." | Reconciliação em níveis (contagem, checksum por partição, diff por chave) automatizada e diária |
# | Copia o DW para o lake | Ingere as **fontes** na bronze; o DW legado não vira fonte permanente |
# | — | Plano de volta, *freeze* de mudanças no legado, dono de negócio que assina a paridade |
# | — | IA para acelerar a conversão de SQL (ex.: Lakebridge, LLM) **com** a reconciliação como teste de aceitação |
#
# > 🎤 **Resposta de 30 s:** "Migração em ondas, não big bang: inventário com linhagem para saber o que é usado,
# > fundação com Unity Catalog e CI/CD, depois domínio a domínio — ingerindo as fontes originais, reescrevendo as
# > transformações e rodando em paralelo com o legado. Paridade com reconciliação automática em níveis: contagem,
# > checksum por partição, diff por chave. O corte é por relatório, com plano de volta, e o legado desliga quando
# > o último consumidor sai."
#
# <details><summary>🔎 Se o entrevistador cavar mais</summary>
#
# - **Diferenças semânticas** que quebram paridade: `NULL` em concatenação, collation (SQL Server é
#   case-insensitive por padrão), arredondamento de `DECIMAL`, fuso horário, `DATETIME` com precisão de 3,33 ms.
# - **Procedures com cursor/loop** viram transformações em conjunto (set-based); não traduza linha a linha.
# - **Power BI:** trocar a fonte para o Databricks SQL (DirectQuery ou import) e validar as medidas DAX com os
#   mesmos números da reconciliação.
# - **Custo:** dual run dobra o custo por um período — negocie a duração com o negócio no início.
# </details>

# %% [markdown]
# ## 6. Runbook de troubleshooting
#
# Formato de cada incidente: **sintoma → onde olhar → causa provável → correção → prevenção**, e uma reprodução
# mínima quando cabe em segundos localmente.
#
# Onde olhar, em geral:
# - **Spark UI** (no Databricks: aba do cluster/job): *Jobs → Stages → Tasks* — duração por tarefa (min/mediana/máx),
#   *shuffle read/write*, *spill* (memória → disco), GC; aba *SQL* — o plano físico executado com métricas por nó.
# - **`DESCRIBE HISTORY`** da tabela Delta: operação, `operationMetrics` (linhas, arquivos, tempo), quem e quando.
# - **Logs do driver/executor** (stderr, log4j), e eventos do cluster (autoscaling, nó perdido).
# - **System tables** ☁️ (`system.lakeflow.job_run_timeline`, `system.billing.usage`, `system.compute.*`,
#   `system.access.audit`): histórico de execuções, custo e auditoria em SQL.

# %% [markdown]
# ### 6.1 O job ficou 3× mais lento
#
# | | |
# |---|---|
# | **Sintoma** | Mesmo código, duração subiu de 20 para 60 min (ou SLA estourou) |
# | **Onde olhar** | Histórico de execuções (system tables / Jobs UI): foi de repente ou gradual? `DESCRIBE HISTORY` das entradas: o volume cresceu? Spark UI: qual estágio cresceu, e nele a tarefa máxima × mediana |
# | **Causa provável** | Gradual → volume cresceu, small files, tabela sem `OPTIMIZE`, estatísticas velhas. De repente → skew (uma chave nova enorme), mudança de plano (broadcast virou sort-merge por a tabela ter passado do limite), cluster menor/spot perdido, upgrade de runtime, filtro que perdeu o pushdown |
# | **Correção** | Comparar o plano físico das duas execuções (aba SQL); tratar a causa (ver 6.3, 6.4, 6.8) |
# | **Prevenção** | Métrica de duração por execução com alerta de desvio; guardar o plano/`operationMetrics` por execução; testes de performance com volume realista antes do deploy |
#
# Reprodução: as métricas que o Delta guarda por operação são a 1ª fonte para comparar execuções.

# %%
hist_path = str(DEMO / "hist")
for n in (10_000, 30_000):  # 2 "execuções" do job, a 2ª com 3× o volume
    bronze.limit(n).write.format("delta").mode("overwrite").save(hist_path)
(DeltaTable.forPath(spark, hist_path).history()
 .select("version", "operation", F.col("operationMetrics.numOutputRows").alias("linhas"),
         F.col("operationMetrics.numFiles").alias("arquivos"),
         F.col("operationMetrics.numOutputBytes").alias("bytes"))
 .orderBy("version").show())

# %% [markdown]
# ### 6.2 OOM no driver × no executor
#
# | | **Driver** | **Executor** |
# |---|---|---|
# | **Sintoma** | `java.lang.OutOfMemoryError` no driver, "Driver is up but not responsive", notebook desconecta | Tarefa falha com `ExecutorLostFailure`, "Container killed... exceeding memory limits", muito *spill* |
# | **Onde olhar** | Código: `collect()`, `toPandas()`, `broadcast()` de tabela grande, loop Python acumulando; Spark UI → Environment | Spark UI → Stages: tarefa com *shuffle read* muito maior que as outras; *spill*; GC alto |
# | **Causa provável** | Trazer dado demais para o driver; broadcast grande (o driver monta a tabela antes de enviar); plano gigante (milhares de colunas, loop de `withColumn`) | Partição grande demais (skew ou poucas partições), `explode` que multiplica linhas, UDF Python com lote grande, `collect_list` sem limite |
# | **Correção** | Agregar no cluster e trazer só o resultado; `limit`/`take`; gravar em tabela em vez de `collect`; `spark.driver.maxResultSize` como guarda | Mais partições (`spark.sql.shuffle.partitions` ou AQE), tratar skew (6.3), filtrar antes de explodir, instância com mais memória por núcleo |
# | **Prevenção** | Revisão de código barrando `collect()`/`toPandas()` sem limite; teste com volume real | Monitorar spill e duração máx/mediana por estágio |
#
# A guarda do driver existe e é configurável — o padrão interrompe o job antes de derrubar o driver:

# %%
print("spark.driver.maxResultSize =", spark.conf.get("spark.driver.maxResultSize", "1g (padrão)"))
print("spark.driver.memory        =", spark.sparkContext.getConf().get("spark.driver.memory"))
amostra = bronze.select("id").limit(5).collect()  # padrão seguro: limitar ANTES de trazer
print("trazido para o driver:", len(amostra), "linhas")

# %% [markdown]
# ### 6.3 Skew (desbalanceamento)
#
# | | |
# |---|---|
# | **Sintoma** | Estágio "parado" em 199/200 tarefas; uma tarefa leva 50× a mediana |
# | **Onde olhar** | Spark UI → Stage → *Summary Metrics*: máx ≫ mediana em duração e *shuffle read*; contagem por chave do join/groupBy |
# | **Causa provável** | Chave dominante (no GH Archive: `github-actions[bot]`), NULL concentrado numa chave de join, partição de data com evento de pico |
# | **Correção** | AQE com *skew join* (padrão no Databricks); broadcast do lado pequeno; filtrar/tratar NULL antes; *salting* (sal aleatório na chave dominante) |
# | **Prevenção** | Conhecer a distribuição das chaves (perfil de dado); testes com o dado real; alerta de tarefa máx/mediana |
#
# Reprodução com o skew natural da bronze — distribuir por ator em 8 partições:

# %%
eventos_ator = bronze.select(F.col("actor.login").alias("login"))


def distribuicao(df, rotulo):
    tam = sorted(r["count"] for r in df.groupBy(F.spark_partition_id().alias("p")).count().collect())
    print(f"{rotulo:28} maior {tam[-1]:>7,} | mediana {tam[len(tam) // 2]:>7,} | maior/mediana {tam[-1] / tam[len(tam) // 2]:.1f}×")


distribuicao(eventos_ator.repartition(8, "login"), "hash(login)")
sal = F.when(F.col("login").endswith("[bot]"), (F.rand(7) * 8).cast("int")).otherwise(F.lit(0))
distribuicao(eventos_ator.withColumn("sal", sal).repartition(8, "login", "sal"), "hash(login, sal) — salting")
print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"), "| skew join:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

# %% [markdown]
# O *salting* espalha os bots por várias partições; o preço é agregar em 2 passos (por `login, sal` e depois por
# `login`) ou replicar o outro lado do join por sal. Tente AQE e broadcast antes — *salting* é o último recurso.
#
# ### 6.4 Small files (arquivos pequenos demais)
#
# | | |
# |---|---|
# | **Sintoma** | Leitura lenta mesmo com pouco dado; listagem demorada; milhares de tarefas minúsculas |
# | **Onde olhar** | `DESCRIBE DETAIL` (`numFiles`, `sizeInBytes`) → tamanho médio; Spark UI: nº de tarefas no scan |
# | **Causa provável** | Streaming com micro-lotes frequentes; `partitionBy` de alta cardinalidade; muitos `INSERT` pequenos; `repartition(200)` antes de gravar pouco dado |
# | **Correção** | `OPTIMIZE` (compacta); *optimized writes* e *auto compaction* ☁️; Liquid Clustering; trigger menos frequente |
# | **Prevenção** | Tamanho médio de arquivo como métrica; `OPTIMIZE` agendado (ou *predictive optimization* ☁️); partição só em coluna de baixa cardinalidade |

# %%
sf = str(DEMO / "small_files")
bronze.select("id", "type", F.col("repo.name").alias("repo"), "created_at").limit(60_000) \
    .repartition(300).write.format("delta").save(sf)


def detalhe_e_tempo(rotulo):
    d = DeltaTable.forPath(spark, sf).detail().first()
    tempos = []
    for _ in range(3):
        t0 = time.perf_counter()
        spark.read.format("delta").load(sf).groupBy("type").count().collect()
        tempos.append(time.perf_counter() - t0)
    print(f"{rotulo:16} arquivos: {d.numFiles:>4} | médio: {d.sizeInBytes / d.numFiles / 1e3:7.1f} KB | "
          f"consulta (mediana de 3): {sorted(tempos)[1]:.2f}s")


detalhe_e_tempo("antes")
spark.sql(f"OPTIMIZE delta.`{sf}`").select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
detalhe_e_tempo("depois OPTIMIZE")

# %% [markdown]
# O `OPTIMIZE` não apaga os arquivos antigos (o *time travel* ainda os usa) — o `VACUUM` remove depois do
# período de retenção (notebook 10).
#
# ### 6.5 Duplicatas após reprocessamento
#
# | | |
# |---|---|
# | **Sintoma** | Contagem dobrou num dia; métricas da gold infladas depois de um "rerun" |
# | **Onde olhar** | `DESCRIBE HISTORY`: duas operações `WRITE`/`APPEND` com o mesmo lote; `GROUP BY chave HAVING COUNT(*) > 1`; `_source_file`/`_ingested_at` repetidos |
# | **Causa provável** | Escrita com `append` não idempotente: o job falhou depois de gravar e o retry gravou de novo; backfill manual por cima |
# | **Correção** | Remover pelo `_ingested_at`/versão (ou `RESTORE` para a versão anterior); passar a escrever com `MERGE` pela chave, ou `replaceWhere` da partição, ou escrita idempotente do Delta (`txnAppId`/`txnVersion`) |
# | **Prevenção** | Toda escrita deve ser idempotente — rodar 2× dá o mesmo resultado; teste automatizado disso (como `test_bronze_e_idempotente`) |

# %%
lote = bronze.select("id", "type", "created_at").orderBy("id").limit(1_000)  # determinístico entre execuções
destinos = {nome: str(DEMO / f"dup_{nome}") for nome in ["append", "merge", "txn"]}
for path in destinos.values():
    lote.limit(0).write.format("delta").save(path)  # tabela vazia com o schema

for _ in range(2):  # o job "rodou 2 vezes" com o mesmo lote
    lote.write.format("delta").mode("append").save(destinos["append"])
    (DeltaTable.forPath(spark, destinos["merge"]).alias("t")
     .merge(lote.alias("s"), "t.id = s.id").whenNotMatchedInsertAll().execute())
    (lote.write.format("delta").mode("append")
     .option("txnAppId", "job_bronze_diario").option("txnVersion", 20261001)  # id do job + nº do lote
     .save(destinos["txn"]))

for nome, path in destinos.items():
    print(f"{nome:7} → {spark.read.format('delta').load(path).count():,} linhas (lote tem 1.000)")

# %% [markdown]
# O `txnAppId`/`txnVersion` faz o Delta ignorar um commit cuja versão de lote já foi gravada por aquele
# aplicativo — é o mecanismo de *exactly-once* do `foreachBatch` (notebook 06). O `MERGE` resolve pela chave; o
# `append` puro duplica.
#
# ### 6.6 A fonte mudou o schema
#
# | | |
# |---|---|
# | **Sintoma** | Job falha com `DELTA_METADATA_MISMATCH`/schema mismatch; ou pior: **não falha** e a coluna nova some, ou vem tudo NULL |
# | **Onde olhar** | Mensagem de erro; `_rescued_data` (Auto Loader) ou `_corrupt_record`; comparar schema da fonte × tabela (`DESCRIBE`); release notes da fonte |
# | **Causa provável** | Coluna nova (aditiva), coluna renomeada, tipo alterado (int → string), aninhamento mudou |
# | **Correção** | Aditiva: `mergeSchema` / `schemaEvolutionMode=addNewColumns` (Auto Loader) — conscientemente. Tipo alterado: não há evolução automática → nova coluna, *type widening* ☁️ quando suportado, ou reescrita da tabela |
# | **Prevenção** | Contrato de dados com o produtor; bronze tolerante (JSON/VARIANT + rescued data) e silver estrita; alerta quando `_rescued_data` não for vazio |

# %%
sch = str(DEMO / "schema")
spark.createDataFrame([(1, "PushEvent")], "id long, type string").write.format("delta").save(sch)
casos = {
    "coluna nova, sem mergeSchema": (spark.createDataFrame([(2, "WatchEvent", "web")], "id long, type string, canal string"), {}),
    "coluna nova, com mergeSchema": (spark.createDataFrame([(3, "ForkEvent", "app")], "id long, type string, canal string"),
                                     {"mergeSchema": "true"}),
    "tipo mudou (id vira string)": (spark.createDataFrame([("x9", "PushEvent")], "id string, type string"), {"mergeSchema": "true"}),
}
for nome, (df, opts) in casos.items():
    try:
        df.write.format("delta").mode("append").options(**opts).save(sch)
        print(f"{nome:32} → ok, schema agora: {spark.read.format('delta').load(sch).columns}")
    except Exception as exc:
        print(f"{nome:32} → falhou: {str(exc).split(']')[0]}]")

# %% [markdown]
# Falhar alto é o comportamento **bom**: a mudança vira decisão de alguém, não corrupção silenciosa.
#
# ### 6.7 Streaming acumulando atraso
#
# | | |
# |---|---|
# | **Sintoma** | O atraso (*lag*) do consumidor cresce sem parar; dashboard "em tempo real" mostra dado de horas atrás |
# | **Onde olhar** | `query.lastProgress` / aba *Structured Streaming* da Spark UI: `inputRowsPerSecond` × `processedRowsPerSecond`, `durationMs` do lote × intervalo do trigger; métrica de atraso do Event Hubs/Kafka (offsets atrás); tamanho do estado |
# | **Causa provável** | Vazão de entrada > vazão de processamento (pico, cluster pequeno); lote cada vez mais lento (estado crescendo sem watermark; MERGE no `foreachBatch` reescrevendo a tabela toda); small files na origem |
# | **Correção** | Escalar (mais partições na fonte + mais núcleos); limitar lote (`maxOffsetsPerTrigger`/`maxFilesPerTrigger`) para estabilizar; watermark para limpar estado; otimizar o `foreachBatch` (pruning, 6.8) |
# | **Prevenção** | Alerta em `processedRowsPerSecond < inputRowsPerSecond` por N lotes e em lag dos offsets; teste de carga no pico esperado |
#
# Reprodução: 4 arquivos na entrada, 1 arquivo por micro-lote — as métricas que dizem se o stream acompanha:

# %%
entrada = DEMO / "stream_in"
for i in range(4):
    bronze.select("id", "type", "created_at").where(F.col("id").substr(-1, 1) == str(i)) \
        .coalesce(1).write.mode("overwrite").json(str(entrada / f"lote_{i}"))
    for f in (entrada / f"lote_{i}").glob("part-*.json"):
        f.rename(entrada / f"lote_{i}.json")
    shutil.rmtree(entrada / f"lote_{i}")

q = (
    spark.readStream.schema("id string, type string, created_at string").option("maxFilesPerTrigger", 1)
    .json(str(entrada))
    .writeStream.format("delta").option("checkpointLocation", str(DEMO / "stream_ckpt"))
    .trigger(availableNow=True).start(str(DEMO / "stream_out"))
)
q.awaitTermination()
for p in q.recentProgress:
    if p["numInputRows"]:
        print(f"lote {p['batchId']}: {p['numInputRows']:>6,} linhas | entrada {p['inputRowsPerSecond']:>9,.0f}/s | "
              f"processado {p['processedRowsPerSecond']:>9,.0f}/s | duração {p['durationMs']['triggerExecution']:>5,} ms")

# %% [markdown]
# Com `availableNow` cada lote começa assim que o anterior termina, então "entrada" e "processado" medem a mesma
# coisa por ângulos diferentes; num stream contínuo, **`processedRowsPerSecond` consistentemente abaixo de
# `inputRowsPerSecond` é a definição de atraso crescendo**.
#
# ### 6.8 MERGE lento
#
# | | |
# |---|---|
# | **Sintoma** | `MERGE` incremental de 10 mil linhas leva tanto quanto reescrever a tabela |
# | **Onde olhar** | `DESCRIBE HISTORY` → `operationMetrics`: `numTargetFilesRemoved` (arquivos reescritos), `numTargetRowsCopied` (linhas reescritas sem mudar), `scanTimeMs` × `rewriteTimeMs`; Spark UI → aba SQL, nó do MERGE: arquivos do alvo antes × depois do *skipping* (quantos foram lidos) |
# | **Causa provável** | Condição só pela chave → o Delta lê a tabela toda para achar os casamentos; as linhas alteradas estão espalhadas por muitos arquivos → reescreve muitos arquivos inteiros; origem com duplicata; skew |
# | **Correção** | Incluir na condição a coluna de partição/clustering (`t.event_date = s.event_date`, ou um filtro literal da janela afetada); Liquid Clustering/Z-order pela chave; *deletion vectors* (marca a linha como apagada em vez de reescrever o arquivo); dedup da origem |
# | **Prevenção** | Acompanhar `numTargetRowsCopied`/`numTargetFilesRemoved` por execução; desenhar a tabela pensando na condição do MERGE |

# %%
base = bronze.select("id", "type", "created_at", F.hour(F.to_timestamp("created_at")).alias("hora")) \
    .where(F.col("id").substr(-1, 1).isin("1", "2", "3"))  # ~30% da bronze, determinístico
atualizacoes = base.where("hora = 14").orderBy("id").limit(500).withColumn("type", F.lit("Atualizado"))

for i, (rotulo, cond) in enumerate([("só a chave", "t.id = s.id"), ("chave + partição", "t.hora = 14 AND t.id = s.id")]):
    mg = str(DEMO / f"merge_{i}")  # cópia nova para cada variante: mesmo layout de partida
    base.repartition(20).write.format("delta").partitionBy("hora").save(mg)
    (DeltaTable.forPath(spark, mg).alias("t").merge(atualizacoes.alias("s"), cond)
     .whenMatchedUpdate(set={"type": "s.type"}).execute())
    m = DeltaTable.forPath(spark, mg).history(1).first()["operationMetrics"]
    print(f"{rotulo:18} arquivos reescritos {m['numTargetFilesRemoved']:>3} | linhas copiadas sem mudar "
          f"{int(m['numTargetRowsCopied']):>6,} | atualizadas {m['numTargetRowsUpdated']} | scan {m['scanTimeMs']} ms")

# O que a condição de partição poupa é a LEITURA: quantos arquivos do alvo são candidatos em cada caso.
tudo = DeltaTable.forPath(spark, mg).history().where("version = 0").first()["operationMetrics"]["numFiles"]
na_particao = len(spark.read.format("delta").option("versionAsOf", 0).load(mg).where("hora = 14").inputFiles())
print(f"arquivos candidatos à leitura — só a chave: {tudo} (tabela toda) | chave + partição: {na_particao}")

# %% [markdown]
# Dois custos separados. **Leitura:** só com a chave, o Delta precisa procurar casamentos na tabela toda; com a
# partição na condição, só nos arquivos daquela partição (*pruning*) — a última linha da saída. **Reescrita:** é a
# mesma nas duas variantes, porque as linhas alteradas são as mesmas: cada arquivo tocado é regravado inteiro, e as
# **linhas copiadas sem mudar** medem esse desperdício — é isso que *deletion vectors* e um bom clustering pela
# chave atacam. (O `scanTimeMs` numa tabela deste tamanho é ruído; a diferença aparece com milhares de arquivos.
# As métricas `numTargetFiles{Before,After}Skipping` aparecem no nó do MERGE na aba SQL da Spark UI, não no
# histórico do Delta open source.)
#
# ### 6.9 `ConcurrentAppendException` (e outros conflitos de concorrência do Delta)
#
# | | |
# |---|---|
# | **Sintoma** | Job falha com `ConcurrentAppendException` / `ConcurrentDeleteReadException` / `ConcurrentTransactionException` |
# | **Onde olhar** | `DESCRIBE HISTORY`: outra operação commitou entre a leitura e o commit deste job (veja `timestamp`, `operation`, `job`) |
# | **Causa provável** | Dois jobs escrevendo na mesma tabela, e um deles leu dados que o outro alterou (ex.: dois `MERGE` sem condição de partição disjunta; `OPTIMIZE` concorrente com `UPDATE`) |
# | **Correção** | Tornar as operações disjuntas na condição (cada job com sua partição explícita: `t.date = '2026-10-01'`); serializar os writers (uma tarefa só por tabela); retry com backoff (é otimista: tentar de novo costuma passar); *row-level concurrency* com deletion vectors/Liquid ☁️ |
# | **Prevenção** | Um dono por tabela; orquestração que não roda 2 escritores na mesma partição; `OPTIMIZE` em janela própria |
#
# Sem reprodução aqui: provocar o conflito exige duas transações intercaladas no tempo certo, o que não é
# determinístico em poucos segundos. O mecanismo (*optimistic concurrency control* e o log) está no notebook 10.
#
# ### 6.10 O custo explodiu
#
# | | |
# |---|---|
# | **Sintoma** | Fatura do mês 2× maior; orçamento estourado |
# | **Onde olhar** | ☁️ `system.billing.usage` (DBU por SKU, workspace, job, tag) × `system.billing.list_prices`; Azure Cost Management (VMs, storage, rede); `system.compute.clusters` (configuração e autoscaling) |
# | **Causa provável** | Cluster all-purpose ligado sem auto-terminate; job em all-purpose em vez de job compute; streaming 24×7 para SLA de horas; autoscaling até o máximo por skew; `VACUUM` nunca rodado (storage); egress entre regiões; consulta de BI sem cache varrendo tabela grande |
# | **Correção** | Auto-terminate, políticas de cluster (*cluster policies*) com teto; job compute/serverless; `availableNow` agendado; tags obrigatórias por time/projeto; `VACUUM` e retenção |
# | **Prevenção** | Orçamento e alerta por tag; revisão de custo no PR (tamanho do cluster no Asset Bundle); painel de custo por pipeline (notebook 15) |
#
# ```sql
# -- ☁️ Top 10 jobs por custo nos últimos 30 dias (DBU × preço de tabela)
# SELECT u.usage_metadata.job_id, SUM(u.usage_quantity * p.pricing.default) AS custo_estimado
# FROM system.billing.usage u
# JOIN system.billing.list_prices p
#   ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
#  AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS AND u.usage_metadata.job_id IS NOT NULL
# GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
# ```
#
# > 🎤 **Resposta de 30 s (para "conte de um incidente"):** use a estrutura **sintoma → onde olhei → causa →
# > correção → o que mudei para não acontecer de novo**. O entrevistador sênior quer ouvir a última parte: o
# > alerta, o teste ou o padrão que você criou — não só o "consertei".

# %% [markdown]
# ## No Databricks / Azure ☁️
#
# ```python
# # Lakeflow Spark Declarative Pipelines: o CDC do caso (b) em poucas linhas (dedup, ordem, delete, SCD)
# from pyspark import pipelines as dp
#
# dp.create_streaming_table("silver_clientes")
# dp.create_auto_cdc_flow(
#     target="silver_clientes", source="bronze_clientes_cdc",
#     keys=["id"], sequence_by=F.col("lsn"),
#     apply_as_deletes=F.expr("op = 'd'"), except_column_list=["op"],
#     stored_as_scd_type=1,            # 2 para histórico
# )
#
# # Event Hubs via protocolo Kafka no Structured Streaming (caso a/c)
# df = (spark.readStream.format("kafka")
#       .option("kafka.bootstrap.servers", "<namespace>.servicebus.windows.net:9093")
#       .option("subscribe", "app-events")
#       .option("kafka.security.protocol", "SASL_SSL")
#       .option("kafka.sasl.mechanism", "PLAIN")
#       .option("kafka.sasl.jaas.config", dbutils.secrets.get("kv-osslh", "eh-jaas"))   # Key Vault
#       .load())
# ```
#
# - **Lakehouse Federation** (caso d): `CREATE CONNECTION ... TYPE sqlserver` + `CREATE FOREIGN CATALOG` para
#   consultar o legado pelo Unity Catalog durante a migração.
# - **Predictive optimization** roda `OPTIMIZE`/`VACUUM` gerenciados em tabelas do Unity Catalog (6.4).
# - **System tables** respondem 6.1 e 6.10 em SQL, sem acessar a Spark UI de execuções antigas.

# %% [markdown]
# ## 7. Perguntas para o candidato fazer ao entrevistador (sinal de senioridade)
#
# Perguntas que mostram que você pensa em operação, time e resultado — escolha 3 ou 4:
#
# **Sobre a plataforma e o dado**
# 1. Qual é o maior problema da plataforma de dados hoje — custo, confiabilidade, velocidade de entrega ou qualidade?
# 2. Como vocês sabem que um dado está errado antes do usuário? Existem contratos/expectations e quem é acionado?
# 3. Quanto do ambiente já está no Unity Catalog e como é a governança de PII (LGPD)?
# 4. Como é o fluxo de deploy: Asset Bundles/Terraform, ambientes dev/hml/prd, quem aprova?
# 5. Batch ou streaming predominam? Existe algum SLA de minutos que doa hoje?
#
# **Sobre o time e o trabalho**
# 6. Como as demandas chegam ao time de dados — produto, squads, fila de tickets? Quem decide a prioridade?
# 7. Como é o plantão (on-call) e qual foi o último incidente relevante? O que mudou depois dele?
# 8. Como o time está usando IA hoje no desenvolvimento e nos pipelines? O que funcionou e o que não?
# 9. O que diferencia um engenheiro sênior de um pleno aqui, na prática?
# 10. O que você esperaria que eu entregasse nos primeiros 90 dias?
#
# Evite perguntar no fim o que estava no anúncio da vaga — e anote as respostas: elas viram material para a
# próxima etapa.

# %% [markdown]
# ## Perguntas de entrevista
#
# **1. Desenhe um pipeline para 1 TB/dia. Por onde começa?**
# <details><summary>Resposta</summary>
# Pelos requisitos (consumidor, latência, retenção, PII) e pela conta: ~1 bilhão de eventos de 1 KB, ~12 mil/s
# na média, ~50 mil/s no pico, centenas de TB em 2 anos. Só então barramento, bronze/silver/gold e o modo de
# disparo. §2.
# </details>
#
# **2. Streaming ou batch?**
# <details><summary>Resposta</summary>
# Pelo SLA e pelo custo: SLA de minutos → streaming (micro-lote); de horas → o mesmo código com
# <code>trigger(availableNow)</code> agendado, cluster desligado entre execuções. Sub-segundo só com requisito real.
# </details>
#
# **3. Como aplicar CDC com deletes e eventos fora de ordem?**
# <details><summary>Resposta</summary>
# Dedup por chave mantendo o maior LSN, <code>MERGE</code> com <code>s.lsn > t.lsn</code>, <code>WHEN MATCHED AND op='d' THEN DELETE</code>, e
# soft delete/tombstone para um update atrasado não ressuscitar a linha. Reaplicar o lote não muda nada. §3.
# </details>
#
# **4. Por que não usar marca d'água por `updated_at` para replicar o ERP?**
# <details><summary>Resposta</summary>
# Não enxerga delete físico, perde updates que não mexem no <code>updated_at</code> (trigger, carga em massa) e consulta as
# tabelas de produção. CDC lê o log de transações.
# </details>
#
# **5. Como você prova que a migração do DW está certa?**
# <details><summary>Resposta</summary>
# Reconciliação automática em níveis — contagem e checksum por partição, depois diff por chave nas partições
# divergentes — em dual run por N dias, com o dono de negócio assinando. Só contagem esconde erros que se
# compensam. §5.
# </details>
#
# **6. O job ficou 3× mais lento sem mudança de código. O que você faz?**
# <details><summary>Resposta</summary>
# Gradual ou súbito? Comparo volume (<code>DESCRIBE HISTORY</code>), plano físico das duas execuções e o estágio que cresceu
# (tarefa máx × mediana). Causas típicas: skew, small files, broadcast que virou sort-merge, cluster diferente.
# </details>
#
# **7. Driver OOM ou executor OOM — como distingue e resolve?**
# <details><summary>Resposta</summary>
# Driver: <code>collect</code>/<code>toPandas</code>/broadcast grande/plano gigante → agregar no cluster, limitar, gravar em tabela.
# Executor: partição grande (skew, poucas partições, explode) → mais partições, AQE, tratar skew, mais memória.
# </details>
#
# **8. Rodaram o job de novo e duplicou. Como corrige e como evita?**
# <details><summary>Resposta</summary>
# Corrige removendo o lote duplicado (ou <code>RESTORE</code>). Evita com escrita idempotente: <code>MERGE</code> pela chave,
# <code>replaceWhere</code> da partição ou <code>txnAppId</code>/<code>txnVersion</code> — reproduzido em §6.5.
# </details>
#
# **9. O MERGE está lento. O que olha no `DESCRIBE HISTORY`?**
# <details><summary>Resposta</summary>
# <code>numTargetFilesRemoved</code> e <code>numTargetRowsCopied</code> (reescrita desnecessária), <code>scanTimeMs</code> ×
# <code>rewriteTimeMs</code>; e, na Spark UI, os arquivos lidos antes × depois do skipping (pruning). Correção: partição/cluster na condição, deletion
# vectors, dedup da origem. §6.8.
# </details>
#
# **10. O que fazer com `ConcurrentAppendException`?**
# <details><summary>Resposta</summary>
# É controle de concorrência otimista: outra transação mudou o que esta leu. Tornar as escritas disjuntas
# (partição explícita na condição), serializar escritores da mesma tabela, retry com backoff.
# </details>
#
# **11. Como detectaria anomalia de volume em até 5 minutos sem afogar o plantão em alertas?**
# <details><summary>Resposta</summary>
# Agregação por minuto em streaming com watermark, z-score contra linha de base sazonal, volume mínimo, duas
# janelas seguidas para disparar, histórico de alertas para medir falso positivo e um runbook por alerta. §4.
# </details>
#
# **12. O custo dobrou. Onde você olha primeiro?**
# <details><summary>Resposta</summary>
# <code>system.billing.usage</code> por job/SKU/tag: cluster all-purpose ligado, streaming 24×7 sem necessidade,
# autoscaling no teto por skew. Depois storage (VACUUM, retenção) e rede. Previne com cluster policies, tags e
# alerta de orçamento.
# </details>

# %% [markdown]
# ## Resumo
#
# - **Pergunte e conte antes de desenhar:** requisitos funcionais e não funcionais, depois eventos/s, MB/s, TB e
#   núcleos — dizendo as premissas.
# - **Cada caixa do diagrama tem um trade-off nomeado:** streaming × `availableNow`, Debezium × Lakeflow Connect × ADF,
#   regra explicável × ML, big bang × ondas.
# - **CDC:** dedup por LSN, `s.lsn > t.lsn`, delete tratado, idempotência provada; delete físico + atraso ressuscita linha.
# - **Migração:** reconciliação em níveis (contagem → checksum → diff por chave) é o critério de aceite.
# - **Incidente:** sintoma → onde olhar (Spark UI, `DESCRIBE HISTORY`, system tables) → causa → correção → **prevenção**.

# %%
spark.stop()
