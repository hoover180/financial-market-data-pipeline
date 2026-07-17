"""
src/transform_dim_securities.py

SCD Type 2 merge logic for dim_securities. Separate from transform.py:
that file does stateless per-batch cleaning on time-series data (Silver
equities/treasury); this is a stateful dimension merge that reads its
own prior output to decide what changed. Different shape, different
pattern -- same reasoning as extract_reference.py vs extract.py.
"""

import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transform_dim_securities")

CATALOG = "financial_market_data"
SCHEMA = "dev"
SNAPSHOT_TABLE = f"{CATALOG}.{SCHEMA}.bronze_dim_securities_snapshot"
DIM_TABLE = f"{CATALOG}.{SCHEMA}.dim_securities"

TRACKED_ATTRS = ["company_name", "sector", "exchange", "asset_type"]


def _table_exists(spark: SparkSession, full_table_name: str) -> bool:
    # spark.table() is lazy over Spark Connect -- it never raises on a
    # missing table until an action runs against it, so a try/except
    # around spark.table() alone always returns True. Use the catalog
    # API, which actually checks existence eagerly.
    return spark.catalog.tableExists(full_table_name)


def _with_security_key(df: DataFrame) -> DataFrame:
    """
    security_key is a random UUID -- a true surrogate key, not derived
    from business data. Guarantees uniqueness across SCD2 versions even
    when a symbol transitions more than once on the same calendar date
    (a deterministic hash of symbol+effective_date would collide in
    that case, which is what the original design did before this fix).
    """
    return df.withColumn("security_key", F.expr("uuid()"))


def apply_scd2_dim_securities(spark: SparkSession) -> dict:
    """
    Compares today's Bronze security snapshot against the current
    (is_current=true) state of dim_securities. Expires rows whose
    tracked attributes changed, then inserts new current versions for
    both changed and brand-new symbols. First run (table doesn't exist
    yet) skips expiry -- every symbol inserts as its first version.

    end_date on expiry is set to today - 1 (not today), so the expired
    and new version's effective ranges are disjoint -- a point-in-time
    query never needs a tiebreaker.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    snapshot = spark.table(SNAPSHOT_TABLE)

    first_run = not _table_exists(spark, DIM_TABLE)

    if first_run:
        logger.info(f"{DIM_TABLE} does not exist -- first run, all symbols insert as v1")
        new_rows = (
            snapshot
            .withColumn("effective_date", F.lit(today))
            .withColumn("end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .select("symbol", *TRACKED_ATTRS, "effective_date", "end_date", "is_current")
        )
        new_rows = _with_security_key(new_rows)

        (
            new_rows.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(DIM_TABLE)
        )

        result = {"inserted": new_rows.count(), "expired": 0, "unchanged": 0, "first_run": True}
        logger.info(f"dim_securities first run: {result}")
        return result

    # --- Not first run: compare incoming snapshot against current state ---
    current = spark.table(DIM_TABLE).filter(F.col("is_current") == True)  # noqa: E712

    # Join incoming snapshot to current rows on symbol. A row needs a new
    # version if it's a brand-new symbol (no current match) OR any
    # tracked attribute differs from the current row.
    compared = snapshot.alias("s").join(current.alias("c"), on="symbol", how="left")

    changed_condition = None
    for attr in TRACKED_ATTRS:
        cond = ~F.col(f"s.{attr}").eqNullSafe(F.col(f"c.{attr}"))
        changed_condition = cond if changed_condition is None else (changed_condition | cond)

    needs_new_version = compared.filter(
        F.col("c.symbol").isNull() | changed_condition
    ).select("s.*")  # just the snapshot's columns, deduplicated by the join

    to_expire = compared.filter(
        F.col("c.symbol").isNotNull() & changed_condition
    ).select("c.security_key")

    expired_count = to_expire.count()
    new_version_count = needs_new_version.count()
    unchanged_count = snapshot.count() - new_version_count

    # Step 1: expire changed rows in place
    if expired_count > 0:
        target = DeltaTable.forName(spark, DIM_TABLE)
        (
            target.alias("t")
            .merge(to_expire.alias("e"), "t.security_key = e.security_key")
            .whenMatchedUpdate(set={
                "end_date": F.lit(yesterday),
                "is_current": F.lit(False),
            })
            .execute()
        )

    # Step 2: insert new current versions for changed + brand-new symbols
    if new_version_count > 0:
        new_rows = (
            needs_new_version
            .withColumn("effective_date", F.lit(today))
            .withColumn("end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .select("symbol", *TRACKED_ATTRS, "effective_date", "end_date", "is_current")
        )
        new_rows = _with_security_key(new_rows)

        (
            new_rows.write
            .format("delta")
            .mode("append")
            .saveAsTable(DIM_TABLE)
        )

    result = {
        "inserted": new_version_count,
        "expired": expired_count,
        "unchanged": unchanged_count,
        "first_run": False,
    }
    logger.info(f"dim_securities merge: {result}")
    return result


if __name__ == "__main__":
    from load import get_spark_session

    spark = get_spark_session()
    try:
        result = apply_scd2_dim_securities(spark)
        print(result)
    finally:
        spark.stop()