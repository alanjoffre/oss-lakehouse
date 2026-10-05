# Key Vault com RBAC (não access policies): permissão por papel, auditável e igual ao resto da Azure.
resource "azurerm_key_vault" "this" {
  name                       = local.kv_name
  resource_group_name        = azurerm_resource_group.this.name
  location                   = azurerm_resource_group.this.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 30
  purge_protection_enabled   = var.env == "prod"

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    ip_rules       = var.allowed_ip_ranges
  }

  tags = local.tags
}

# Quem roda o Terraform administra os segredos (para gravar, ex., o token da API do GitHub).
resource "azurerm_role_assignment" "kv_admin_deployer" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Secret scope do Databricks "apoiado" no Key Vault: dbutils.secrets.get("kv", "github-token")
# lê direto do cofre; o valor nunca aparece em notebook nem em log (é mascarado como [REDACTED]).
resource "databricks_secret_scope" "kv" {
  count = var.create_kv_secret_scope ? 1 : 0
  name  = "kv"

  keyvault_metadata {
    resource_id = azurerm_key_vault.this.id
    dns_name    = azurerm_key_vault.this.vault_uri
  }
}

# Com Key Vault em modo RBAC, quem lê os segredos do scope é o app primário "AzureDatabricks".
resource "azurerm_role_assignment" "kv_databricks_reader" {
  count                = var.create_kv_secret_scope && var.azure_databricks_sp_object_id != null ? 1 : 0
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.azure_databricks_sp_object_id
}
