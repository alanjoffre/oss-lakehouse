# Estado remoto (exemplo). Estado local serve para estudo; em time, o estado fica num Storage
# Account dedicado, com lock por lease de blob (dois `apply` simultâneos não corrompem nada)
# e autenticação por Entra ID (sem chave da conta). Um estado por ambiente (key diferente).
#
# terraform {
#   backend "azurerm" {
#     resource_group_name  = "rg-tfstate"
#     storage_account_name = "sttfstateosslh"
#     container_name       = "tfstate"
#     key                  = "oss-lakehouse/dev.tfstate"
#     use_azuread_auth     = true
#   }
# }
#
# Uso: terraform init -backend-config="key=oss-lakehouse/prod.tfstate"
