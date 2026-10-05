SHELL := /bin/bash
SPARK := scripts/spark_slot.sh
export SPARK_LOCAL_IP ?= 127.0.0.1

.PHONY: help setup data bronze test lint notebooks nb demo tf-validate azurite-up azurite-down clean-lake

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

demo: data bronze ## Demo de ponta a ponta para entrevista (offline depois do make data)
	$(SPARK) uv run python -m oss_lakehouse.cli demo

tf-validate: ## Valida o Terraform da Azure sem credenciais
	cd infra/terraform/azure && terraform init -backend=false -input=false >/dev/null && terraform fmt -check -recursive && terraform validate

azurite-up: ## Sobe o emulador do Azure Storage
	docker compose -f infra/azurite/docker-compose.yml up -d

azurite-down: ## Derruba o emulador
	docker compose -f infra/azurite/docker-compose.yml down

clean-lake: ## Apaga as tabelas locais (mantém os downloads)
	rm -rf data/bronze data/silver data/gold data/quarantine data/_checkpoints data/demo
