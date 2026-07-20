output "schema_full_name" {
  description = "Fully qualified name of the Terraform-managed schema"
  value       = databricks_schema.terraform_managed.id
}