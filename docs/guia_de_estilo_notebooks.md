# Guia de estilo dos notebooks

Todo notebook deste repositório é, ao mesmo tempo, **referência técnica** e **evidência executada**.
Quem lê precisa sair sabendo *o que é*, *por que existe*, *como funciona* e *quando não usar* — e ver rodando.

## Formato da fonte

- Fonte em `notebooks/_src/NN_nome.py`, formato jupytext *percent* (`# %%` e `# %% [markdown]`).
- `scripts/build_notebooks.py NN` converte para `notebooks/NN_nome.ipynb` **e executa** — as saídas vão para o Git.
- Nunca editar o `.ipynb` à mão: ele é gerado.

## Estrutura obrigatória

```text
# NN · Título
> Uma frase: o que este notebook prova.
Tabela "Tema → onde aparece aqui" (curta).
Legenda: 🧪 roda local · ☁️ só no Databricks/Azure (código mostrado, não executado aqui)

## Setup              (imports, get_spark(), caminhos via get_settings())

## 1. <Tópico>        (repetir para cada tópico)
  **O que é** — definição curta, com o termo de mercado em inglês explicado em português.
  **Por que importa** — o problema real que resolve.
  **Como funciona** — o mecanismo (diagrama ASCII/mermaid quando ajudar).
  <célula(s) de código>  — curtas, com saída que PROVA o ponto (contagem, plano, tempo, antes/depois).
  > **Em resumo:** a ideia central em 3 ou 4 linhas.
  <details><summary>🔎 Para aprofundar</summary> … </details>
  **Trade-offs / quando NÃO usar** — bullets.

## No Databricks / Azure ☁️    (o que muda na plataforma real — código em bloco markdown)

## Perguntas técnicas
  Cada pergunta em negrito, resposta dentro de <details>. 6 a 12 perguntas, das básicas às avançadas.

## Resumo                (3–5 bullets com o essencial)
```

## Regras

- **Idioma:** pt-br. Termos técnicos em inglês quando é assim que o mercado fala (shuffle, skew, watermark), sempre explicados na 1ª aparição.
- **Tom:** didático e direto, de engenheiro para engenheiro. Sem marketing, sem "incrível".
- **Honestidade:** o que não roda local é marcado ☁️ e não é "simulado como se rodasse". Nada de número inventado: todo número no texto saiu de uma célula executada.
- **Evidência > afirmação:** "o broadcast eliminou o shuffle" vem com o `explain()` antes/depois ou o tempo medido.
- **Saídas pequenas:** `show(5)`, `limit`, `truncate`. Nada de imprimir milhares de linhas (o .ipynb vai para o Git).
- **Execução rápida:** cada notebook roda inteiro em poucos minutos, offline, a partir de `make data` (dados já baixados).
- **Lógica no pacote:** regra de negócio/transformação reaproveitável vai para `src/oss_lakehouse/` com teste em `tests/`. O notebook chama e explica. Código de demonstração descartável pode ficar no notebook.
- **Caminhos:** sempre `get_settings().path(...)`. Demonstração destrutiva (DROP, VACUUM, RESTORE, sobrescrever) usa `data/demo/NN/` — **nunca** as tabelas compartilhadas.
- **Spark:** `from oss_lakehouse.spark import get_spark; spark = get_spark("NN")`. Última célula: `spark.stop()`.
