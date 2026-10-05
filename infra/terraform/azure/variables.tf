variable "subscription_id" {
  description = "Subscription da Azure onde o ambiente vive."
  type        = string
}

variable "env" {
  description = "Ambiente: dev ou prod (entra no nome dos recursos e no nome do catálogo)."
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env deve ser dev ou prod."
  }
}

variable "location" {
  description = "Região da Azure. O metastore do Unity Catalog é regional: workspace e storage na mesma região."
  type        = string
  default     = "brazilsouth"
}

variable "project" {
  description = "Prefixo curto do projeto (letras minúsculas e números)."
  type        = string
  default     = "osslh"

  validation {
    condition     = can(regex("^[a-z0-9]{3,8}$", var.project))
    error_message = "project: 3 a 8 caracteres, minúsculas e números (nome de Storage Account é restrito)."
  }
}

variable "storage_replication" {
  description = "LRS em dev (barato); ZRS em prod (sobrevive à queda de uma zona)."
  type        = string
  default     = "LRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "GZRS"], var.storage_replication)
    error_message = "Use LRS, ZRS, GRS ou GZRS."
  }
}

variable "allowed_ip_ranges" {
  description = "IPs/CIDRs liberados no firewall do Storage e do Key Vault (vazio = só redes confiáveis/privadas)."
  type        = list(string)
  default     = []
}

variable "public_network_access" {
  description = "Acesso público ao Storage: Enabled (dev) ou Disabled (prod, só private endpoint)."
  type        = string
  default     = "Enabled"
}

variable "groups" {
  description = "Grupos de conta do Databricks (sincronizados do Entra ID via SCIM) usados nos grants."
  type = object({
    engineers = string
    analysts  = string
  })
  default = {
    engineers = "data-engineers"
    analysts  = "data-analysts"
  }
}

variable "cluster_max_workers" {
  description = "Teto de workers da cluster policy — o principal freio de custo de compute clássico."
  type        = number
  default     = 4
}

variable "cluster_autotermination_minutes" {
  description = "Cluster ocioso desliga sozinho depois de N minutos."
  type        = number
  default     = 20
}

variable "create_kv_secret_scope" {
  description = "Cria o secret scope do Databricks apontando para o Key Vault (exige login de usuário Entra ID, não service principal)."
  type        = bool
  default     = false
}

variable "cost_center" {
  description = "Centro de custo para a tag de FinOps."
  type        = string
  default     = "engenharia-de-dados"
}

variable "owner" {
  description = "Time dono dos recursos (tag)."
  type        = string
  default     = "time-dados"
}

variable "azure_databricks_sp_object_id" {
  description = "Object ID (no SEU tenant) do app primário 'AzureDatabricks' (app id 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d), que lê o Key Vault no secret scope."
  type        = string
  default     = null
}
