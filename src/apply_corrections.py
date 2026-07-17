"""
src/apply_corrections.py

Detects and applies late-arriving corrections to Silver equities data.

Framing: for batch-pulled market data, "late-arriving" means a
previously-loaded historical row gets revised by the source (yfinance
backfills a corrected close, a stock split retroactively adjusts
historical prices) -- not out-of-order streaming arrival. Separate
from transform.py: this is a stateful diff-and-MERGE operation against
the table's own current values, not a stateless per-batch clean.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("apply_corrections")

CATALOG = "financial_market_data"
SCHEMA = "dev"
SILVER_EQUITIES = f"{CATALOG}.{SCHEMA}.silver_equities"
CORRECTION_LOG = f"{CATALOG}.{SCHEMA}.correction_log"

CORRECTION_FIELDS = ["open", "high", "low", "close", "volume"]


def _fresh_equities_frame(spark: SparkSession, window_days: int = 30) -> DataFrame:
    """
    Pulls a recent trailing window (default 30 days) of equities data,
    not full history -- real-world vendor corrections/backfills are
    almost always recent, so re-checking all of history on every run
    would be wasteful API usage for negligible additional coverage.
    30 days is a judgment call, not derived from vendor SLAs -- generous
    enough to catch typical correction lag, cheap enough to run often.

    Runs through prepare_dataframe() + transform_equities(), the same
    path Bronze/Silver already use -- not a separate casting
    implementation. Ensures the fresh pull's schema, symbol casing, and
    dedup logic can never drift from what's actually in silver_equities,
    which would otherwise risk false-positive "corrections" from a
    formatting mismatch rather than a real vendor revision.
    """
    from datetime import date, timedelta
    from extract import load_sources_config, extract_equities
    from load import prepare_dataframe
    from transform import transform_equities

    sources = load_sources_config()
    equities_cfg = next(s for s in sources if s["name"] == "equities")

    # Windowed config: same source, but start_date overridden to the
    # trailing window instead of sources.yml's full-history start_date.
    windowed_cfg = dict(equities_cfg)
    windowed_cfg["start_date"] = (date.today() - timedelta(days=window_days)).isoformat()

    fresh_pd = extract_equities(windowed_cfg)

    fresh_bronze_shaped = prepare_dataframe(spark, fresh_pd, "equities")
    return transform_equities(fresh_bronze_shaped).select(
        "symbol", "date", *CORRECTION_FIELDS
    )


def detect_and_apply_corrections(spark: SparkSession, window_days: int = 30) -> dict:
    """
    Compares a fresh trailing-window equities extraction (default last
    30 days) against the corresponding slice of silver_equities. Any
    (symbol, date) row where open, high, low, close, or volume differs
    from what's stored is logged to correction_log (one row per changed
    field, for a precise before/after audit trail) and applied via
    Delta MERGE.

    Windowed, not full-history: real-world vendor revisions are almost
    always recent. Only corrects existing history within the window --
    rows with no diff, and dates not yet present in Silver, are left
    untouched. This is not a first-time loader; that's the normal
    Bronze/Silver pipeline's job.

    MERGE here is a deliberate exception to Silver's full-replace
    convention, same class of exception as dim_securities' SCD2 append
    -- see docs/data_modeling_decisions.md.
    """
    now = datetime.now(timezone.utc)

    fresh = _fresh_equities_frame(spark, window_days=window_days)
    current = spark.table(SILVER_EQUITIES).select("symbol", "date", *CORRECTION_FIELDS)

    for f in CORRECTION_FIELDS:
        fresh = fresh.withColumnRenamed(f, f"new_{f}")
    for f in CORRECTION_FIELDS:
        current = current.withColumnRenamed(f, f"old_{f}")

    compared = fresh.join(current, on=["symbol", "date"], how="inner")

    diff_condition = None
    for f in CORRECTION_FIELDS:
        cond = ~F.col(f"new_{f}").eqNullSafe(F.col(f"old_{f}"))
        diff_condition = cond if diff_condition is None else (diff_condition | cond)

    corrected = compared.filter(diff_condition)
    corrected_count = corrected.count()

    if corrected_count == 0:
        logger.info("No corrections detected -- fresh extraction matches silver_equities exactly")
        return {"corrected_rows": 0, "fields_logged": 0}

    logger.info(f"{corrected_count} row(s) with at least one changed field detected")

    # One correction_log row per changed field, not per record -- a
    # precise audit trail rather than a coarse "this row changed".
    log_parts = []
    for f in CORRECTION_FIELDS:
        part = (
            corrected
            .filter(~F.col(f"new_{f}").eqNullSafe(F.col(f"old_{f}")))
            .select(
                F.col("symbol"),
                F.col("date"),
                F.lit(f).alias("field_changed"),
                F.col(f"old_{f}").cast("string").alias("old_value"),
                F.col(f"new_{f}").cast("string").alias("new_value"),
            )
        )
        log_parts.append(part)

    log_df = log_parts[0]
    for part in log_parts[1:]:
        log_df = log_df.unionByName(part)
    log_df = log_df.withColumn("corrected_at", F.lit(now))

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(CORRECTION_LOG)
    )
    fields_logged = log_df.count()

    target = DeltaTable.forName(spark, SILVER_EQUITIES)
    update_set = {f: F.col(f"s.new_{f}") for f in CORRECTION_FIELDS}
    (
        target.alias("t")
        .merge(corrected.alias("s"), "t.symbol = s.symbol AND t.date = s.date")
        .whenMatchedUpdate(set=update_set)
        .execute()
    )

    result = {"corrected_rows": corrected_count, "fields_logged": fields_logged}
    logger.info(f"corrections applied: {result}")
    return result


if __name__ == "__main__":
    from load import get_spark_session

    spark = get_spark_session()
    try:
        result = detect_and_apply_corrections(spark, window_days=30)
        print(result)
    finally:
        spark.stop()