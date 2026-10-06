# Documentação

| Documento | Para quê |
|---|---|
| [Decisões de arquitetura (ADRs)](adr/README.md) | Por que o sistema é assim: contexto, decisão, alternativas e consequências de cada escolha |
| [Diagramas](diagramas.md) | Os diagramas de arquitetura dos notebooks, renderizados (arquitetura alvo na Azure, casos de system design, CI/CD) |
| [Contratos das tabelas](contratos_de_tabelas.md) | Esquema, grão, chave e dono de cada tabela das camadas bronze, silver e gold |
| [Trilha de notebooks](plano_notebooks.md) | Os 18 notebooks e o tema de cada um |
| [Guia de estilo dos notebooks](guia_de_estilo_notebooks.md) | O formato que todo notebook segue e as regras de evidência |

Fora desta pasta:

- [`../README.md`](../README.md) — visão geral, arquitetura e como rodar.
- [`../GUIA_DE_ESTUDO.md`](../GUIA_DE_ESTUDO.md) — as perguntas de todos os notebooks, com resposta.
- [`../evals/revisao_humana/README.md`](../evals/revisao_humana/README.md) — como validar os gabaritos das etapas com LLM.
- [`../infra/terraform/azure`](../infra/terraform/azure) — a infraestrutura da Azure como código.
