"""
transform.py — Silver layer: type casting, deduplication, standardization.

Reads Bronze Delta tables, applies conformance transformations, writes
Silver Delta tables. No data-quality remediation here — Bronze is already
clean per the Phase 2 ad hoc validation. This layer standardizes shape and
types only.

Non-null enforcement on key columns (symbol/date, series_id/date) lives
at the Delta table level via a one-time ALTER TABLE ... SET NOT NULL
(see sql/silver_constraints.sql) -- not re-derived here on every run.
This module only does a cheap pre-write check for a clear error message;
Delta itself is the actual enforcement point for any writer, not just
this script.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType

BRONZE_EQUITIES = "financial_market_data.dev.bronze_equities"
BRONZE_TREASURY = "financial_market_data.dev.bronze_treasury_yields"
SILVER_EQUITIES = "financial_market_data.dev.silver_equities"
SILVER_TREASURY = "financial_market_data.dev.silver_treasury_yields"

# Key columns per table -- must match the NOT NULL columns declared in
# sql/silver_constraints.sql. Used only for the pre-write check below;
# the table itself is the actual enforcement point.
SILVER_KEY_COLUMNS = {
    "equities": ["symbol", "date"],
    "treasury_yields": ["series_id", "date"],
}


def check_not_null(df: DataFrame, table_key: str) -> DataFrame:
    """
    Fails fast with a specific column/count before the write, rather
    than surfacing Delta's less legible DeltaInvariantViolationException
    once the table-level NOT NULL constraint rejects the write.
    """
    key_cols = SILVER_KEY_COLUMNS[table_key]

    null_counts = (
        df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in key_cols])
        .collect()[0]
        .asDict()
    )
    bad_cols = {c: n for c, n in null_counts.items() if n > 0}
    if bad_cols:
        raise ValueError(f"{table_key}: nulls found in key column(s): {bad_cols}")

    return df


def transform_equities(df: DataFrame) -> DataFrame:
    """Standardize equities Bronze -> Silver."""
    df = (
        df
        # Ticker casing: defensive against future lowercase entries in
        # sources.yml, not a fix for current data.
        .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        # Explicit casts as a Silver-layer type contract, independent of
        # Bronze's schema.
        .withColumn("date", F.col("date").cast(DateType()))
        .withColumn("open", F.col("open").cast(DoubleType()))
        .withColumn("high", F.col("high").cast(DoubleType()))
        .withColumn("low", F.col("low").cast(DoubleType()))
        .withColumn("close", F.col("close").cast(DoubleType()))
        .withColumn("volume", F.col("volume").cast(LongType()))
    )

    # Defensive uniqueness guarantee on (symbol, date); Bronze already
    # confirmed zero duplicates (Phase 2).
    df = df.dropDuplicates(["symbol", "date"])

    df = df.select("symbol", "date", "open", "high", "low", "close", "volume")
    return check_not_null(df, "equities")


def transform_treasury(df: DataFrame) -> DataFrame:
    """Standardize treasury yields Bronze -> Silver."""
    df = (
        df
        .withColumn("series_id", F.upper(F.trim(F.col("series_id"))))
        .withColumn("date", F.col("date").cast(DateType()))
        .withColumn("value", F.col("value").cast(DoubleType()))
        # value remains nullable by design -- ~4.23% null rate is
        # holiday-driven, not a defect (see data_dictionary.md).
    )

    df = df.dropDuplicates(["series_id", "date"])

    df = df.select("series_id", "date", "value")
    return check_not_null(df, "treasury_yields")


def run_silver_transform(spark: SparkSession) -> None:
    """Entry point: read Bronze, transform, write Silver (overwrite mode)."""
    bronze_equities = spark.table(BRONZE_EQUITIES)
    bronze_treasury = spark.table(BRONZE_TREASURY)

    silver_equities = transform_equities(bronze_equities)
    silver_treasury = transform_treasury(bronze_treasury)

    (silver_equities.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_EQUITIES))

    (silver_treasury.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TREASURY))

    print(f"Silver equities: {silver_equities.count()} rows written to {SILVER_EQUITIES}")
    print(f"Silver treasury: {silver_treasury.count()} rows written to {SILVER_TREASURY}")


if __name__ == "__main__":
    from load import get_spark_session  # reuse existing session helper

    spark = get_spark_session()
    try:
        run_silver_transform(spark)
    finally:
        spark.stop()