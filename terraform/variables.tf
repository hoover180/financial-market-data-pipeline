variable "catalog_name" {
  type        = string
  description = "Unity Catalog catalog to create the schema in"
  default     = "financial_market_data"
}

variable "schema_name" {
  type        = string
  description = "Name of the Terraform-managed schema"
  default     = "terraform_managed"
}

variable "grant_principal" {
  type        = string
  description = "Databricks account email or group to grant USE_SCHEMA/SELECT to"
}