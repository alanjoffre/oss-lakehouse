# ADR 0003 — `payload` como JSON bruto na bronze

- **Status:** Aceito
- **Data:** 2026-10-05

## Contexto

Cada evento do GH Archive tem um envelope estável (`id`, `type`, `actor`, `repo`, `org`, `created_at`) e um `payload`
cujo formato depende do `type` (16 tipos: PushEvent, PullRequestEvent, IssuesEvent…). O GitHub muda esses payloads sem
aviso — campos novos, campos removidos, tipos que mudam.

Se a bronze inferir o schema do `payload`:

- a inferência lê o dado inteiro (lento) e o resultado varia com a amostra;
- um campo novo vira coluna nova (ou quebra a escrita, se a evolução de schema estiver desligada);
- um campo que muda de tipo (número → objeto) quebra a leitura do dia inteiro.

## Decisão

Na bronze, só o **envelope** tem schema explícito (`GH_EVENT_SCHEMA` em `oss_lakehouse.bronze`). O `payload` é
gravado como **STRING com o JSON bruto**. Quem interpreta é a silver, por tipo de evento, com `from_json` e schema
declarado por tipo (notebook 05). Linhas que não casam com o envelope não derrubam o job (`mode=PERMISSIVE`).

## Alternativas consideradas

- **Inferir o schema completo** (`spark.read.json` sem schema): rápido de escrever, frágil em produção.
- **Schema evolution automática** (`mergeSchema`/`autoMerge`): aceita colunas novas, mas não resolve mudança de tipo
  e deixa a tabela com centenas de colunas esparsas. Desligado de propósito em `get_spark()`.
- **Tipo VARIANT** (Databricks, Delta 4.x, Spark 4): guarda semiestruturado em binário eficiente e consulta com
  `payload:commits[0].sha`. É a evolução natural desta decisão no Databricks; não adotado por padrão porque
  o objetivo da bronze aqui é ser legível e portável em qualquer leitor de Parquet/Delta.
- **Auto Loader com `rescuedDataColumn`**: guarda o que não casou numa coluna de resgate. Usado em conjunto,
  no Databricks (notebook 03).

## Consequências

- (+) A bronze nunca quebra por mudança no payload; reprocessar a silver com regra nova é sempre possível.
- (+) Contrato da bronze pequeno e estável (`docs/contratos_de_tabelas.md`).
- (−) Consultar a bronze diretamente exige `get_json_object`/`from_json` — custo de parse em cada leitura.
  Ninguém deveria consultar a bronze para análise; é para isso que existe a silver.
- (−) Storage maior que colunar tipado (string JSON comprime pior).
