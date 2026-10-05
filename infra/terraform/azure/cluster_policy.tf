# Cluster policy: o "guard-rail" de custo e de governança do compute clássico.
# Fixa o modo de acesso compatível com UC, limita tamanho e obriga autodesligamento e tags.
resource "databricks_cluster_policy" "jobs" {
  name        = "oss-lakehouse-jobs-${var.env}"
  description = "Clusters de job do oss-lakehouse: UC, autoscaling limitado, tags de custo"

  definition = jsonencode({
    "data_security_mode" : { "type" : "fixed", "value" : "USER_ISOLATION" },
    "spark_version" : { "type" : "unlimited", "defaultValue" : "auto:latest-lts" },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["Standard_D4ds_v5", "Standard_D8ds_v5", "Standard_E8ds_v5"],
      "defaultValue" : "Standard_D4ds_v5"
    },
    "autoscale.max_workers" : { "type" : "range", "maxValue" : var.cluster_max_workers, "defaultValue" : 2 },
    "autotermination_minutes" : {
      "type" : "range", "maxValue" : 60, "defaultValue" : var.cluster_autotermination_minutes
    },
    "azure_attributes.availability" : { "type" : "fixed", "value" : "SPOT_WITH_FALLBACK_AZURE" },
    "custom_tags.project" : { "type" : "fixed", "value" : var.project },
    "custom_tags.environment" : { "type" : "fixed", "value" : var.env },
    "custom_tags.cost_center" : { "type" : "fixed", "value" : var.cost_center }
  })
}

resource "databricks_permissions" "jobs_policy" {
  cluster_policy_id = databricks_cluster_policy.jobs.id

  access_control {
    group_name       = var.groups.engineers
    permission_level = "CAN_USE"
  }
}
