"""
Sets up the Great Expectations file-backed context and Databricks SQL
datasource used for Phase 6 data quality checks.

Requires DBT_DATABRICKS_TOKEN in the environment, scoped to sql + unity-catalog
(a sql-only-scoped PAT is rejected by direct connector sessions).

Safe to re-run: uses add_or_update_databricks_sql and falls back to
get_asset() if the table asset already exists in gx/great_expectations.yml.
"""

import os

import great_expectations as gx
from databricks import sql

# --- connection config ---
token = os.environ["DBT_DATABRICKS_TOKEN"]
host = "dbc-6b255145-0361.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/b9517f9e3bf14a31"
catalog = "financial_market_data"
schema = "dev"

connection_string = (
    "databricks://token:${DATABRICKS_TOKEN}@"
    f"{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
)

# --- raw connector sanity check (bypasses GE/SQLAlchemy entirely) ---
conn = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=token,
)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchall())
conn.close()

# --- GE context + datasource ---
context = gx.get_context(mode="file")  # creates ./gx/ on first run

datasource = context.data_sources.add_or_update_databricks_sql(
    name="databricks_datasource",
    connection_string=connection_string,
)

try:
    table_asset = datasource.add_table_asset(
        name="dim_securities_current_asset",
        table_name="dim_securities_current",
    )
except gx.exceptions.exceptions.DataContextError:
    table_asset = datasource.get_asset("dim_securities_current_asset")

batch_definition = table_asset.add_batch_definition_whole_table(
    "dim_securities_current_batch"
)
batch = batch_definition.get_batch()
print(batch.head())  # should show 3 rows: SPY, QQQ, AAPL