# Terraform — Infrastructure as Code

## What this provisions

Two Unity Catalog resources, demonstrating the storage + access-control pattern:

- `databricks_schema.terraform_managed` — a governed schema under the `financial_market_data` catalog (the storage-equivalent resource)
- `databricks_grants.terraform_managed_grants` — a `USE_SCHEMA` + `SELECT` grant on that schema (the IAM-equivalent resource)

## Why Databricks-native resources instead of cloud provider resources (S3/IAM)

This project's infrastructure lives entirely inside Databricks/Unity Catalog rather than a separate cloud account. Rather than introduce a second cloud account and billing surface just to hit a literal bucket-and-IAM-role checkbox, the same storage-plus-access-control pattern was mapped onto Unity Catalog's native resources.

## Why Terraform for two resources

Infrastructure deployment is integrated into the same CI/CD-minded discipline as the rest of this project, so the environment is fully reproducible rather than manually clicked together. Two resources are enough to demonstrate that pattern without inflating scope for its own sake.

## What a full production build would additionally provision (not built here)

- SQL warehouse sizing/autoscaling configuration
- Network/private-link configuration
- Workspace-level admin settings
- Secret scopes
- Cluster policies

These are either not exposed on Databricks Free Edition or intentionally out of scope for a two-resource IaC demonstration.

## Auth and state

- Provider authenticates via the existing `financial_market_data_pipeline` CLI profile (OAuth) — no tokens in any committed file.
- State is local (`terraform.tfstate`), gitignored.
- The one non-default variable (`grant_principal`) is supplied via a gitignored `terraform.tfvars`; `terraform.tfvars.example` shows the expected shape.

## Lifecycle verified

Full `destroy` → `apply` cycle exercised on 2026-07-19 to confirm reproducibility, not just a one-time success.
