"""
src/load.py
 
Loads validated records into Databricks Bronze Delta tables via
Databricks Connect (Spark), targeting serverless compute.
"""

import os
import logging
from datetime import datetime, timezone

import pandas as pd
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("load")

CATALOG = "financial_market_data"
SCHEMA = "dev"
DEFAULT_PROFILE = "financial_market_data_pipeline"

# Explicit schemas per Bronze table -- avoids misinference on
# partially-null columns, e.g. treasury `value`, which has real gaps
# from FRED (holidays / unreported days).
BRONZE_SCHEMAS = {
    "equities": StructType([
        StructField("symbol", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ]),
    "treasury_yields": StructType([
        StructField("series_id", StringType(), False),
        StructField("date", DateType(), False),
        StructField("value", DoubleType(), True),
    ]),
}


def get_spark_session(profile: str = None) -> "DatabricksSession":
    """
    Opens a Spark session against Databricks serverless compute via
    Databricks Connect, using a named profile from ~/.databrickscfg.

    Falls back to the DEFAULT profile / environment-variable-based auth
    if no profile name is given and DATABRICKS_CONFIG_PROFILE isn't set,
    matching Databricks Connect's own resolution order.
    """
    profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE", DEFAULT_PROFILE)

    try:
        return (
            DatabricksSession.builder
            .serverless()
            .profile(profile)
            .getOrCreate()
        )
    except Exception as e:
        raise EnvironmentError(
            f"Failed to open Databricks Connect session using profile "
            f"'{profile}'. Confirm ~/.databrickscfg has this profile "
            f"defined with host, token, and serverless_compute_id = auto, "
            f"and that `databricks-connect test` passes. Original error: {e}"
        ) from e


def prepare_dataframe(spark, df: pd.DataFrame, table_key: str) -> SparkDataFrame:
    """
    Converts a validated pandas DataFrame into a Spark DataFrame with an
    explicit schema, normalizing the date column to a proper date type
    (pandas can round-trip dates as strings or Timestamps depending on
    which extract path produced them).
    """
    schema = BRONZE_SCHEMAS[table_key]
    expected_cols = [f.name for f in schema.fields]

    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise ValueError(f"{table_key}: missing expected columns {missing}")

    df = df[expected_cols].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    return spark.createDataFrame(df, schema=schema)


def write_bronze(
    spark,
    df: pd.DataFrame,
    table_key: str,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
    mode: str = "overwrite",
) -> dict:
    """
    Writes a DataFrame to a Bronze Delta table in Unity Catalog.

    mode="overwrite" is deliberate, not a placeholder: both sources
    (yfinance, FRED) revise historical values after the fact -- adjusted
    close prices shift with stock splits, FRED republishes revised
    economic data -- so an append-only Bronze layer would preserve stale
    values instead of corrections. Trade-off: no run-to-run audit history
    at this layer. See docs/data_modeling_decisions.md for the full
    load-strategy rationale.

    Uses overwriteSchema (not mergeSchema): fails fast on schema drift
    rather than silently accumulating stale columns, consistent with
    Bronze/Silver as current-state layers (see
    docs/data_modeling_decisions.md). Primary schema enforcement is
    upstream in prepare_dataframe() via explicit StructType -- this is
    the defensive second layer, not the main guarantee.
    """
    full_table_name = f"{catalog}.{schema}.bronze_{table_key}"
    spark_df = prepare_dataframe(spark, df, table_key)

    row_count = spark_df.count()

    (
        spark_df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    result = {
        "table": full_table_name,
        "rows_loaded": row_count,
        "mode": mode,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        f"table={full_table_name} rows_loaded={row_count} "
        f"mode={mode} timestamp={result['timestamp']}"
    )
    return result


def load_all(extracted: dict, spark=None, profile: str = None) -> list:
    """
    Loads a dict of {table_key: pandas.DataFrame} -- as returned by
    src/extract.py's run_extraction(), post-validation -- into their
    corresponding Bronze Delta tables.

    Opens and closes its own Spark session unless one is passed in
    (useful for tests or when chaining multiple loads in one run).
    """
    own_session = spark is None
    if own_session:
        spark = get_spark_session(profile=profile)

    results = []
    try:
        for table_key, df in extracted.items():
            if table_key not in BRONZE_SCHEMAS:
                logger.warning(f"No Bronze schema defined for '{table_key}', skipping.")
                continue
            if df.empty:
                logger.warning(f"'{table_key}' has no valid rows to load, skipping.")
                continue
            results.append(write_bronze(spark, df, table_key))
    finally:
        if own_session:
            spark.stop()

    return results


if __name__ == "__main__":
    from extract import run_extraction
    from validate import validate_records

    extracted = run_extraction()

    validated = {}
    for table_key, df in extracted.items():
        schema_file = "equities_schema.json" if table_key == "equities" else "treasury_schema.json"
        schema_path = f"config/schemas/{schema_file}"
        valid_rows, flagged = validate_records(df, schema_path, table_key)
        validated[table_key] = pd.DataFrame(valid_rows)

    load_results = load_all(validated)
    for r in load_results:
        print(r)