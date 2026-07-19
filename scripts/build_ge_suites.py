"""
Builds the five Great Expectations suites for Phase 6 data quality checks.

Depends on the same Databricks connection config used in
scripts/setup_ge_context.py. Safe to re-run: uses add_or_update_databricks_sql
for the datasource and get_or_create helpers for assets/suites, since GE's
file-backed context (./gx/) persists state across runs and raises
DataContextError on a second attempt to add something that already exists.

Requires DBT_DATABRICKS_TOKEN in the environment, scoped to sql + unity-catalog
(a sql-only-scoped PAT is rejected by direct connector sessions).
"""

from datetime import datetime, timedelta

import great_expectations as gx
import great_expectations.expectations as gxe

# --- connection config ---
host = "dbc-6b255145-0361.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/b9517f9e3bf14a31"
catalog = "financial_market_data"
schema = "dev"

connection_string = (
    "databricks://token:${DATABRICKS_TOKEN}@"
    f"{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
)

# --- context + datasource ---
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


def get_or_create_suite(name):
    try:
        return context.suites.add(gx.ExpectationSuite(name=name))
    except gx.exceptions.exceptions.DataContextError:
        return context.suites.get(name)


freshness_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

# =====================================================================
# silver_equities
# =====================================================================
equities_asset = get_or_create_asset("silver_equities_asset", "silver_equities")
equities_batch_def = equities_asset.add_batch_definition_whole_table(
    "silver_equities_batch"
)

equities_suite = get_or_create_suite("silver_equities_suite")
equities_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="symbol"))
equities_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="date"))
equities_suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0, max_value=None)
)
equities_suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0, max_value=None)
)
equities_suite.add_expectation(
    gxe.ExpectColumnMaxToBeBetween(
        column="date", min_value=freshness_cutoff, max_value=None
    )
)

# =====================================================================
# silver_treasury_yields
# =====================================================================
treasury_asset = get_or_create_asset(
    "silver_treasury_yields_asset", "silver_treasury_yields"
)
treasury_batch_def = treasury_asset.add_batch_definition_whole_table(
    "silver_treasury_yields_batch"
)

treasury_suite = get_or_create_suite("silver_treasury_yields_suite")
treasury_suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="series_id", value_set=["DGS10", "DGS2"])
)
treasury_suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="value", min_value=0, max_value=20)
)
treasury_suite.add_expectation(
    gxe.ExpectColumnMaxToBeBetween(
        column="date", min_value=freshness_cutoff, max_value=None
    )
)

# =====================================================================
# fct_daily_returns
# =====================================================================
returns_asset = get_or_create_asset("fct_daily_returns_asset", "fct_daily_returns")
returns_batch_def = returns_asset.add_batch_definition_whole_table(
    "fct_daily_returns_batch"
)

returns_suite = get_or_create_suite("fct_daily_returns_suite")
returns_suite.add_expectation(gxe.ExpectTableRowCountToEqual(value=4920))
# mostly=0.99 tolerates rare legitimate extreme-move days rather than hard-failing
# the whole suite on a single outlier; first-trading-day NaN (no prior close) is
# expected and excluded from this check by design, matching the dbt not_null
# test's treatment of the same column.
returns_suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="daily_return", min_value=-0.5, max_value=0.5, mostly=0.99
    )
)

# =====================================================================
# fct_market_yield_daily
# =====================================================================
yield_daily_asset = get_or_create_asset(
    "fct_market_yield_daily_asset", "fct_market_yield_daily"
)
yield_daily_batch_def = yield_daily_asset.add_batch_definition_whole_table(
    "fct_market_yield_daily_batch"
)

yield_daily_suite = get_or_create_suite("fct_market_yield_daily_suite")
yield_daily_suite.add_expectation(gxe.ExpectTableRowCountToEqual(value=4920))
yield_daily_suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="treasury_10y_yield", min_value=0, max_value=20
    )
)

# =====================================================================
# dim_securities_current
# =====================================================================
dim_asset = get_or_create_asset(
    "dim_securities_current_asset", "dim_securities_current"
)
dim_batch_def = dim_asset.add_batch_definition_whole_table(
    "dim_securities_current_batch"
)

dim_suite = get_or_create_suite("dim_securities_current_suite")
dim_suite.add_expectation(gxe.ExpectTableRowCountToEqual(value=3))
dim_suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="asset_type", value_set=["EQUITY", "ETF"])
)
dim_suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="symbol"))

print("All five suites created/updated successfully.")