# ADLS Gen2 = Storage Account com namespace hierárquico (HNS). HNS dá diretórios de verdade
# (rename atômico de pasta, ACL POSIX) e o endpoint DFS que o driver ABFS (abfss://) usa.
resource "azurerm_storage_account" "lake" {
  name                     = local.storage_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.storage_replication
  access_tier              = "Hot"
  is_hns_enabled           = true

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  # Sem chave da conta: acesso só por identidade (Entra ID + RBAC). Chave vazada = conta inteira.
  shared_access_key_enabled       = false
  default_to_oauth_authentication = true
  public_network_access           = var.public_network_access

  network_rules {
    default_action = "Deny"
    # AzureServices: deixa serviços confiáveis (ex.: Access Connector) passarem pelo firewall.
    bypass   = ["AzureServices"]
    ip_rules = var.allowed_ip_ranges
  }

  blob_properties {
    # Soft delete em blob/container: o "lixo" para o dia em que alguém apaga a pasta errada.
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

# Um container "lake" com pastas landing/bronze/silver/gold (mesmo layout do config.py local:
# data_root = abfss://lake@<conta>.dfs.core.windows.net) + um container só para o storage
# GERENCIADO do catálogo do Unity Catalog. Ver o notebook 14 para a justificativa.
resource "azurerm_storage_container" "lake" {
  name                  = "lake"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "uc_managed" {
  name                  = "uc-managed"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}
