"""
tests/verify.py — reproducible spot-check of Bronze and Silver tables.
Formalizes the Phase 2 Bronze ad hoc validation (previously untracked)
and the Phase 3A Silver check into one rerunnable script. Not a Great
Expectations check (that's Phase 6) -- a manual sanity check, structured
so it survives being run again after any future extract/load/transform.

Not named test_*.py so pytest's default discovery won't pick it up --
this is a manual tool, not part of the automated test suite (yet).
"""

import sys
from pathlib import Path
from pyspark.sql import functions as F

# tests/ and src/ are siblings, not nested -- add src/ to the path
# explicitly so this resolves the same way regardless of where/how
# the script is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from load import get_spark_session
from extract import load_sources_config


def layer_header(name: str) -> None:
    print(f"\n{'=' * 50}\n{name}\n{'=' * 50}")


def section(title: str) -> None:
    print(f"\n--- {title} ---")


def get_equities_symbols():
    sources = load_sources_config()
    return next(s["symbols"] for s in sources if s["name"] == "equities")


def verify_dim_securities_snapshot(spark, symbols: list[str]) -> None:
    section("dim_securities_snapshot: row count, dupes, company_name check")
    df = spark.sql(
        "SELECT * FROM financial_market_data.dev.bronze_dim_securities_snapshot"
    )
    count = df.count()
    assert count == len(symbols), f"Expected {len(symbols)} rows, got {count}"

    dupes = df.groupBy("symbol").count().filter("count > 1").count()
    assert dupes == 0, f"Duplicate symbol rows found: {dupes}"

    null_names = df.filter(df.company_name.isNull()).count()
    assert null_names == 0, "company_name should never be null"

    print(f"dim_securities_snapshot: {count} rows, no dupes, company_name populated — OK")


def verify_bronze(spark) -> None:
    layer_header("BRONZE")

    section("Row counts (expect 4,920 / 3,408)")
    spark.sql("SELECT COUNT(*) AS bronze_equities FROM financial_market_data.dev.bronze_equities").show()
    spark.sql("SELECT COUNT(*) AS bronze_treasury FROM financial_market_data.dev.bronze_treasury_yields").show()

    section("Duplicate key check, equities (expect zero rows)")
    spark.sql("""
        SELECT symbol, date, COUNT(*) c
        FROM financial_market_data.dev.bronze_equities
        GROUP BY symbol, date HAVING c > 1
    """).show()

    section("Duplicate key check, treasury (expect zero rows)")
    spark.sql("""
        SELECT series_id, date, COUNT(*) c
        FROM financial_market_data.dev.bronze_treasury_yields
        GROUP BY series_id, date HAVING c > 1
    """).show()

    section("OHLC internal consistency (expect zero rows: low <= open/close <= high)")
    spark.sql("""
        SELECT * FROM financial_market_data.dev.bronze_equities
        WHERE NOT (low <= open AND open <= high AND low <= close AND close <= high)
    """).show()

    section("Negative or zero prices/volume (expect zero rows)")
    spark.sql("""
        SELECT * FROM financial_market_data.dev.bronze_equities
        WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume <= 0
    """).show()

    section("Treasury null rate (expect ~0.0423 for both series)")
    spark.sql("""
        SELECT series_id,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_rate
        FROM financial_market_data.dev.bronze_treasury_yields
        GROUP BY series_id
    """).show()

    section("Schema check (symbol/date and series_id/date should show nullable = false, "
            "per load.py's BRONZE_SCHEMAS)")
    spark.table("financial_market_data.dev.bronze_equities").printSchema()
    spark.table("financial_market_data.dev.bronze_treasury_yields").printSchema()

    # dim_securities_snapshot lives in Bronze too -- verified as part of
    # this layer's check, not a separate scope.
    symbols = get_equities_symbols()
    verify_dim_securities_snapshot(spark, symbols)


def verify_silver(spark) -> None:
    layer_header("SILVER")

    section("Row count parity vs Bronze (expect 4,920 / 3,408, matching Bronze exactly)")
    spark.sql("SELECT COUNT(*) AS silver_equities FROM financial_market_data.dev.silver_equities").show()
    spark.sql("SELECT COUNT(*) AS silver_treasury FROM financial_market_data.dev.silver_treasury_yields").show()

    section("Duplicate key check, equities (expect zero rows)")
    spark.sql("""
        SELECT symbol, date, COUNT(*) c
        FROM financial_market_data.dev.silver_equities
        GROUP BY symbol, date HAVING c > 1
    """).show()

    section("Treasury null rate, unchanged from Bronze (expect ~0.0423 for both series)")
    spark.sql("""
        SELECT series_id,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_rate
        FROM financial_market_data.dev.silver_treasury_yields
        GROUP BY series_id
    """).show()

    section("Schema check (symbol/date and series_id/date should show nullable = false "
            "once sql/silver_constraints.sql has been run)")
    spark.table("financial_market_data.dev.silver_equities").printSchema()
    spark.table("financial_market_data.dev.silver_treasury_yields").printSchema()

    # dim_securities is derived/modeled data (SCD2), not raw Bronze --
    # verified here alongside Silver, not in verify_bronze().
    symbols = get_equities_symbols()
    verify_dim_securities(spark, symbols)


def verify_dim_securities(spark, symbols: list[str]) -> None:
    section("dim_securities: SCD2 integrity check")
    df = spark.sql("SELECT * FROM financial_market_data.dev.dim_securities")

    current_counts = (
        df.filter(F.col("is_current") == True)  # noqa: E712
        .groupBy("symbol")
        .count()
    )
    bad_counts = current_counts.filter(F.col("count") != 1)
    bad_count_rows = bad_counts.count()
    assert bad_count_rows == 0, (
        f"{bad_count_rows} symbol(s) do not have exactly one is_current=true row"
    )

    current_symbols = {row["symbol"] for row in current_counts.select("symbol").collect()}
    missing = set(symbols) - current_symbols
    assert not missing, f"Symbols missing a current row entirely: {missing}"

    orphans = df.filter(
        (F.col("is_current") == False) & F.col("end_date").isNull()  # noqa: E712
    ).count()
    assert orphans == 0, f"{orphans} expired row(s) missing end_date"

    open_ended_current = df.filter(
        (F.col("is_current") == True) & F.col("end_date").isNotNull()  # noqa: E712
    ).count()
    assert open_ended_current == 0, (
        f"{open_ended_current} current row(s) incorrectly have an end_date set"
    )

    total = df.count()
    print(
        f"dim_securities: {total} total row(s) across {len(current_symbols)} symbol(s), "
        f"exactly one is_current=true per symbol, no orphaned end_dates — OK"
    )


if __name__ == "__main__":
    layers = sys.argv[1:] or ["bronze", "silver"]  # default: run both

    spark = get_spark_session()
    try:
        if "bronze" in layers:
            verify_bronze(spark)
        if "silver" in layers:
            verify_silver(spark)
    finally:
        spark.stop()