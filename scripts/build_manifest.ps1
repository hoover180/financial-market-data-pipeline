# build_manifest.ps1
# Stages profiles.yml, builds the dbt image with a BuildKit secret (token
# never touches an image layer), extracts the generated manifest.json for
# Cosmos's LoadMode.MANIFEST, then cleans up the staged profile.

$ErrorActionPreference = "Stop"

if (-not $env:DBT_DATABRICKS_TOKEN) {
    Write-Error "DBT_DATABRICKS_TOKEN is not set in this session. Set it before running this script."
    exit 1
}

# 1. Stage profiles.yml into the build context (gitignored, temporary)
Copy-Item "$env:USERPROFILE\.dbt\profiles.yml" ".\profiles.yml" -Force

# 2. Build with BuildKit secret — token read from env, never baked into a layer
docker build -f Dockerfile.dbt -t financial-pipeline-dbt:latest `
    --secret id=dbt_token,env=DBT_DATABRICKS_TOKEN .

if ($LASTEXITCODE -ne 0) {
    Remove-Item ".\profiles.yml" -Force
    Write-Error "docker build failed."
    exit 1
}

# 3. Extract the manifest for Cosmos (LoadMode.MANIFEST reads this from the
#    Airflow side, via the existing ./dbt volume mount)
docker create --name temp_manifest_extract financial-pipeline-dbt:latest | Out-Null
docker cp temp_manifest_extract:/dbt/target/manifest.json `
    ".\dbt\financial_market_data_pipeline\target\manifest.json"
docker rm temp_manifest_extract | Out-Null

# 4. Clean up the staged profile — never leave a copy sitting in the repo folder
Remove-Item ".\profiles.yml" -Force

Write-Host "Done. Image built, manifest extracted, staged profile removed." -ForegroundColor Green