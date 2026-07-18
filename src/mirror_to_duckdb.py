"""
Mirrors all Bronze/Silver/Dimension/Audit Delta tables into a local DuckDB
file for dev-loop validation without spinning up serverless Databricks
compute for every query iteration. Read-only mirror — never a source of
truth, never written back upstream. Rerun freely; each run fully replaces
the local copy (same overwrite-on-refresh convention as Bronze/Silver
upstream, chosen for the same reason: fails fast, no silent staleness from
partial syncs).
"""
import duckdb
from databricks.connect import DatabricksSession
from pathlib import Path

DUCKDB_PATH = Path(__file__).resolve().parent.parent / "data" / "local_dev.duckdb"

# local table name -> fully qualified Unity Catalog source
TABLES = {
    "bronze_dim_securities_snapshot": "financial_market_data.dev.bronze_dim_securities_snapshot",
    "bronze_equities":                "financial_market_data.dev.bronze_equities",
    "bronze_treasury_yields":         "financial_market_data.dev.bronze_treasury_yields",
    "correction_log":                 "financial_market_data.dev.correction_log",
    "dim_securities":                 "financial_market_data.dev.dim_securities",
    "silver_equities":                "financial_market_data.dev.silver_equities",
    "silver_treasury_yields":         "financial_market_data.dev.silver_treasury_yields",
}

def mirror():
    DUCKDB_PATH.parent.mkdir(exist_ok=True)
    spark = DatabricksSession.builder.getOrCreate()
    con = duckdb.connect(str(DUCKDB_PATH))

    for local_name, source_table in TABLES.items():
        pdf = spark.table(source_table).toPandas()
        con.execute(f"CREATE OR REPLACE TABLE {local_name} AS SELECT * FROM pdf")
        print(f"{local_name}: {len(pdf)} rows mirrored from {source_table}")

    con.close()
    print(f"\nMirror complete -> {DUCKDB_PATH}")

if __name__ == "__main__":
    mirror()