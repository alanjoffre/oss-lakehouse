# Versões FIXADAS: provider muda schema entre majors (ex.: azurerm 4 → 5 renomeou atributos do
# Key Vault). Pin exato + `terraform init -upgrade` deliberado, revisado em PR.
terraform {
  required_version = ">= 1.9.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "5.8.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "1.136.0"
    }
  }
}
