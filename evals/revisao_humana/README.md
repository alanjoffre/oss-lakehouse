# Revisão humana dos gabaritos, às cegas

Os gabaritos em `evals/` foram propostos por um assistente de IA. Um modelo da mesma família é o avaliado, então
a acurácia medida contra eles tende a sair **inflada** — os dois erram parecido. O remédio é um segundo rotulador
independente, humano, e uma medida de concordância entre os dois.

## Por que às cegas

- **Ancoragem:** quem revisa vendo o rótulo existente tende a concordar com ele.
- **Vazamento:** corrigir só os itens em que o modelo errou melhora a métrica sem melhorar o modelo.

Por isso as planilhas **não mostram** o gabarito nem a resposta do modelo.

## Como fazer

1. Abrir `titulos_as_cegas.csv` e preencher a coluna `rotulo_humano` com uma destas categorias:

   | Categoria | Quando usar |
   |---|---|
   | `bug` | relato ou correção de comportamento incorreto (erro, falha, regressão, vulnerabilidade, problema de desempenho relatado) |
   | `feature` | funcionalidade nova ou melhoria de comportamento visível (inclui melhoria de desempenho proposta) |
   | `docs` | documentação, especificação, README, comentários |
   | `chore` | manutenção sem mudança de comportamento: dependência, CI/build, versão, refatoração, testes |
   | `outro` | não é mudança de software nem defeito (conteúdo, tradução de conteúdo, anúncio, pergunta, título vago) |

   Regras do critério (as mesmas que o modelo recebe): prefixo de *conventional commits* manda
   (`fix`→bug, `feat`→feature, `docs`→docs, os demais→chore); atualização de dependência é sempre `chore`.
   Título em idioma que você não lê: `nao_sei` (fica fora da conta, e o relatório diz quantos foram).

   A ordem das linhas é aleatória: se não der para rotular os 120, rotule os primeiros 40 — continua sendo uma
   amostra aleatória.

2. Abrir `pii_as_cegas.csv` e preencher `pii_humano` com `sim` ou `nao`: a coluna contém dado pessoal (identifica
   ou pode identificar uma pessoa, direta ou indiretamente)? As amostras estão mascaradas de propósito.

3. Medir:

   ```bash
   make revisao-medir
   ```

   Sai a concordância bruta com intervalo de confiança, o **kappa de Cohen** (concordância descontado o acaso) e
   a lista das divergências.

## Como ler o resultado

- **kappa > 0,8:** o gabarito é confiável; os números do notebook 12 podem ser citados como concordância com um
  gabarito validado por humano.
- **kappa entre 0,6 e 0,8:** razoável; olhar as divergências — costumam apontar categoria mal definida.
- **kappa < 0,6:** o problema é o critério, não o modelo. Reescrever as definições antes de avaliar qualquer coisa.

Divergência não significa que o gabarito está errado nem que o humano está: é o ponto de partida para decidir,
caso a caso, e **registrar a decisão**. Se o gabarito mudar, o notebook 12 precisa ser reexecutado.
