# Access Connector = recurso Azure que carrega uma identidade gerenciada (managed identity)
# para o Unity Catalog. O Databricks acessa o storage COMO essa identidade: sem segredo para rodar.
resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-${local.suffix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Menor privilégio: papel de DADOS (data plane) só nesta conta — não "Contributor" (control plane).
resource "azurerm_role_assignment" "uc_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
  principal_type       = "ServicePrincipal"
}

# SKU premium: requisito para Unity Catalog com controle de acesso fino, e para cluster policies,
# audit logs e IP access lists. Workspaces novos já nascem com UC habilitado (metastore regional
# atribuído automaticamente).
resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${local.suffix}"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = "rg-${local.suffix}-dbw-managed"

  # no_public_ip = true (secure cluster connectivity): os nós não têm IP público.
  # Produção acrescenta VNet injection (custom_parameters com virtual_network_id + sub-redes
  # pública/privada) e private endpoints — omitidos aqui para manter o exemplo enxuto.
  custom_parameters {
    no_public_ip = true
  }

  tags = local.tags
}
