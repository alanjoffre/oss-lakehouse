locals {
  # Convenção de nomes no estilo do Cloud Adoption Framework: <tipo>-<projeto>-<ambiente>-<região>.
  suffix = "${var.project}-${var.env}"
  # Storage Account e Key Vault: nome global, sem hífen (storage), até 24 caracteres.
  storage_name = substr(replace("st${var.project}${var.env}lake", "-", ""), 0, 24)
  kv_name      = substr("kv-${local.suffix}", 0, 24)

  # Tags em TUDO: é por elas que o Cost Management separa a fatura por projeto/ambiente.
  tags = {
    project     = var.project
    environment = var.env
    cost_center = var.cost_center
    owner       = var.owner
    managed_by  = "terraform"
  }

  catalog_name = "oss_lakehouse_${var.env}"
  layers       = ["bronze", "silver", "gold"]

  lake_url    = "abfss://${azurerm_storage_container.lake.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  managed_url = "abfss://${azurerm_storage_container.uc_managed.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
}
