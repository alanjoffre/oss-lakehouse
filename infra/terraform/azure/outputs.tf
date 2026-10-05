output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}

output "data_root" {
  description = "Valor de OSSLH_DATA_ROOT no Databricks (mesmo código, outra raiz)."
  value       = local.lake_url
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "access_connector_principal_id" {
  value = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

output "key_vault_uri" {
  value = azurerm_key_vault.this.vault_uri
}

output "catalog" {
  value = databricks_catalog.this.name
}

output "cluster_policy_id" {
  value = databricks_cluster_policy.jobs.id
}
