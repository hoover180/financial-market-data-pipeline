"""
Builds and runs a Great Expectations checkpoint bundling all five Phase 6
validation definitions, then generates Data Docs as the final artifact.

Depends on scripts/build_ge_suites.py having run at least once (suites must
already exist in ./gx/expectations/).
"""

import great_expectations as gx

# --- connection config (same as build_ge_suites.py) ---
host = "dbc-6b255145-0361.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/b9517f9e3bf14a31"
catalog = "financial_market_data"
schema = "dev"

connection_string = (
    "databricks://token:${DATABRICKS_TOKEN}@"
    f"{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
)

context = gx.get_context(mode="file")
datasource = context.data_sources.add_or_update_databricks_sql(
    name="databricks_datasource",
    connection_string=connection_string,
)


def get_or_create_asset(name, table_name):
    try:
        return datasource.add_table_asset(name=name, table_name=table_name)
    except gx.exceptions.exceptions.DataContextError:
        return datasource.get_asset(name)


def get_batch_definition(asset, batch_def_name):
    try:
        return asset.add_batch_definition_whole_table(batch_def_name)
    except gx.exceptions.exceptions.DataContextError:
        return asset.get_batch_definition(batch_def_name)


def get_or_create_validation_definition(name, batch_definition, suite):
    try:
        return context.validation_definitions.add(
            gx.ValidationDefinition(name=name, data=batch_definition, suite=suite)
        )
    except gx.exceptions.exceptions.DataContextError:
        return context.validation_definitions.get(name)


# --- rebuild the five (asset, batch_definition, suite) triples ---
table_configs = [
    ("silver_equities_asset", "silver_equities", "silver_equities_batch", "silver_equities_suite"),
    ("silver_treasury_yields_asset", "silver_treasury_yields", "silver_treasury_yields_batch", "silver_treasury_yields_suite"),
    ("fct_daily_returns_asset", "fct_daily_returns", "fct_daily_returns_batch", "fct_daily_returns_suite"),
    ("fct_market_yield_daily_asset", "fct_market_yield_daily", "fct_market_yield_daily_batch", "fct_market_yield_daily_suite"),
    ("dim_securities_current_asset", "dim_securities_current", "dim_securities_current_batch", "dim_securities_current_suite"),
]

validation_defs = []
for asset_name, table_name, batch_def_name, suite_name in table_configs:
    asset = get_or_create_asset(asset_name, table_name)
    batch_def = get_batch_definition(asset, batch_def_name)
    suite = context.suites.get(suite_name)
    vd = get_or_create_validation_definition(f"{suite_name}_validation", batch_def, suite)
    validation_defs.append(vd)

# --- checkpoint bundling all five ---
try:
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="financial_market_data_checkpoint",
            validation_definitions=validation_defs,
        )
    )
except gx.exceptions.exceptions.DataContextError:
    checkpoint = context.checkpoints.get("financial_market_data_checkpoint")

results = checkpoint.run()
print(results.describe())

# --- Data Docs artifact ---
context.build_data_docs()
context.open_data_docs()