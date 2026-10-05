provider "azurerm" {
  # Desde o azurerm 4.x a subscription é obrigatória (aqui ou em ARM_SUBSCRIPTION_ID).
  subscription_id = var.subscription_id
  # Data plane do Storage (containers) por Entra ID, não por chave da conta.
  storage_use_azuread = true

  features {
    key_vault {
      # Em produção NÃO purgar no destroy: o soft delete é a rede de segurança dos segredos.
      purge_soft_delete_on_destroy = false
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}

# Provider do Databricks no nível do WORKSPACE, apontando para o workspace criado aqui.
# Ressalva conhecida: provider configurado com atributo de recurso criado no mesmo apply só
# funciona bem no primeiro apply se o workspace existir antes — em produção, separe em duas
# stacks (infra Azure → objetos do Databricks) ou use `-target` no bootstrap.
provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}
