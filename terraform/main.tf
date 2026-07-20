provider "databricks" {
  profile = "financial_market_data_pipeline"
}

resource "databricks_schema" "terraform_managed" {
  catalog_name = var.catalog_name
  name         = var.schema_name
  comment      = "Provisioned via Terraform — Phase 7 IaC demonstration"
}

resource "databricks_grants" "terraform_managed_grants" {
  schema = databricks_schema.terraform_managed.id

  grant {
    principal  = var.grant_principal
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}