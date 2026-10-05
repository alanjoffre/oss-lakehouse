# Unity Catalog: storage credential (a identidade) → external location (o caminho) → catálogo.
resource "databricks_storage_credential" "lake" {
  name    = "sc-${local.suffix}"
  comment = "Identidade gerenciada do Access Connector (Terraform)"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc.id
  }

  depends_on = [azurerm_role_assignment.uc_storage]
}

resource "databricks_external_location" "lake" {
  name            = "el-${local.suffix}-lake"
  url             = local.lake_url
  credential_name = databricks_storage_credential.lake.name
  comment         = "Landing e dados externos (arquivos que chegam de fora)"
}

resource "databricks_external_location" "uc_managed" {
  name            = "el-${local.suffix}-managed"
  url             = local.managed_url
  credential_name = databricks_storage_credential.lake.name
  comment         = "Storage gerenciado do catálogo"
}

resource "databricks_catalog" "this" {
  name         = local.catalog_name
  comment      = "Lakehouse do ecossistema open source (${var.env})"
  storage_root = databricks_external_location.uc_managed.url
  properties = {
    environment = var.env
  }
}

resource "databricks_schema" "layer" {
  for_each     = toset(local.layers)
  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = "Camada ${each.key} do Medallion"
}

# Grants: grupos, nunca usuários individuais. Engenharia escreve; análise só lê a gold.
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name

  grant {
    principal  = var.groups.engineers
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }
  grant {
    principal  = var.groups.analysts
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "schema" {
  for_each = databricks_schema.layer
  schema   = each.value.id

  grant {
    principal  = var.groups.engineers
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }

  dynamic "grant" {
    for_each = each.key == "gold" ? [1] : []
    content {
      principal  = var.groups.analysts
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

resource "databricks_grants" "lake_location" {
  external_location = databricks_external_location.lake.id

  grant {
    principal  = var.groups.engineers
    privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
  }
}
