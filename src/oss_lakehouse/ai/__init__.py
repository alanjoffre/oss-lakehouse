"""IA aplicada à engenharia de dados: LLM como etapa de pipeline, com contrato e avaliação.

- `client`: interface `LLMClient` + provedores `cache` (padrão, offline), `claude_cli`, `anthropic`.
- `prompts`: prompts versionados em `prompts/*.toml`.
- `pii`, `titles`, `quality`, `docgen`, `triage`: os cinco casos de uso do notebook 12.
- `evaluation`: métricas e gates de CI; `batch`: inferência em lote no Spark; `pricing`: custo.

Respostas gravadas ficam em `cache/respostas.jsonl` (versionado): notebook e testes rodam
sem rede e sem custo, e o mesmo pedido nunca é pago duas vezes.
"""
