# ADR 0006 — IA no pipeline só com avaliação e humano no circuito

- **Status:** Aceito
- **Data:** 2026-10-05

## Contexto

Modelos de linguagem (LLMs) ajudam na engenharia de dados: classificar texto livre (mensagens de commit, títulos de
issue), detectar dado pessoal (PII), propor regras de qualidade, documentar tabelas e colunas. Mas um LLM é
**não determinístico**, erra com confiança, custa por token e pode vazar dado se o texto for para fora.
Um erro do modelo dentro de um pipeline se propaga para todas as camadas seguintes sem ninguém ver.

## Decisão

Toda etapa com IA no pipeline (notebook 12) segue quatro regras:

1. **Avaliação antes de produção:** conjunto de referência rotulado (*golden set*), métrica definida
   (precisão/recall/F1 para classificação; taxa de acerto para extração) e um limiar mínimo. Mudou o prompt ou o
   modelo → roda a avaliação de novo (é teste de regressão).
2. **Saída estruturada e validada:** o modelo responde num schema (JSON validado por Pydantic); resposta fora do
   schema vai para quarentena, não para a silver.
3. **Humano no circuito** (*human-in-the-loop*) para o que muda regra ou contrato: regra de qualidade e documentação
   sugeridas pela IA entram como **proposta** (PR revisado), nunca aplicadas sozinhas.
4. **Rastreabilidade e custo:** cada linha enriquecida guarda modelo, versão do prompt e data; tokens e custo
   medidos por execução. Cache por hash da entrada para não pagar duas vezes pelo mesmo texto.

Dado pessoal é mascarado **antes** de ir ao modelo; o que é detectado como PII é governado no Unity Catalog
(notebook 11).

## Alternativas consideradas

- **IA aplicada direto, sem avaliação** ("o modelo parece bom"): rápido e impossível de defender; regressão invisível.
- **Só regras determinísticas** (regex, listas): previsíveis e baratas; usadas como primeira barreira e como base
  de comparação — a IA tem de bater a regra para entrar.
- **Modelo próprio treinado** (ML clássico): melhor custo por item em volume alto; exige dado rotulado em quantidade
  que não temos. Revisitar se o volume justificar.
- No Databricks, as mesmas regras valem para **AI Functions** (`ai_query`, `ai_classify`, `ai_extract`) e para o
  **Databricks Assistant**: a ferramenta muda, a disciplina de avaliação não.

## Consequências

- (+) Dá para responder "como você sabe que funciona?" com número.
- (+) Troca de modelo/fornecedor vira decisão medida, não aposta.
- (−) Custo e tempo para manter o conjunto rotulado e a avaliação.
- (−) Latência e custo por item limitam o uso a colunas e volumes onde a regra determinística não alcança.
