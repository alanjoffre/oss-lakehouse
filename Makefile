SHELL := /bin/bash
SPARK := scripts/spark_slot.sh
export SPARK_LOCAL_IP ?= 127.0.0.1

.PHONY: help setup data bronze test lint notebooks nb guia diagramas revisao-medir demo build bundle-validate precommit tf-validate azurite-up azurite-down clean-lake

help: ## Lista os comandos
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

setup: ## Instala dependências (precisa de Java 17 e uv)
	uv sync

data: ## Baixa o GH Archive de 2026-10-01 (3 horas na landing, o dia inteiro no cache)
	uv run python -m oss_lakehouse.cli download

bronze: ## Ingere a landing na bronze (incremental, idempotente)
	$(SPARK) uv run python -m oss_lakehouse.cli bronze

test: ## Testes unitários e de integração (Spark local)
	$(SPARK) uv run pytest -q

lint: ## Ruff
	uv run ruff check src tests scripts

notebooks: ## Gera e executa todos os notebooks
	$(SPARK) uv run python scripts/build_notebooks.py

nb: ## Gera e executa um notebook: make nb N=05
	$(SPARK) uv run python scripts/build_notebooks.py $(N)

guia: ## Regenera o GUIA_DE_ESTUDO.md a partir dos notebooks
	uv run python scripts/build_guia.py

diagramas: ## Regenera docs/diagramas.md (Mermaid dos notebooks, renderizado pelo GitHub)
	uv run python scripts/build_diagramas.py

revisao-medir: ## Mede a concordância da revisão humana dos gabaritos de IA
	uv run python scripts/revisao_gabarito.py medir

build: ## Gera o wheel do pacote (o artefato que o job do Databricks instala)
	uv build --wheel

bundle-validate: ## Valida o bundle do Databricks (precisa de workspace configurado)
	databricks bundle validate -t dev

precommit: ## Roda os hooks de pre-commit em todos os arquivos
	uvx pre-commit run --all-files

demo: data ## Demo de ponta a ponta para entrevista (offline depois do make data)
	$(SPARK) uv run python -m oss_lakehouse.cli demo

tf-validate: ## Valida o Terraform da Azure sem credenciais
	cd infra/terraform/azure && terraform init -backend=false -input=false >/dev/null && terraform fmt -check -recursive && terraform validate

azurite-up: ## Sobe o emulador do Azure Storage
	docker compose -f infra/azurite/docker-compose.yml up -d

azurite-down: ## Derruba o emulador
	docker compose -f infra/azurite/docker-compose.yml down

clean-lake: ## Apaga as tabelas locais (mantém os downloads)
	rm -rf data/bronze data/silver data/gold data/quarantine data/_checkpoints data/demo
