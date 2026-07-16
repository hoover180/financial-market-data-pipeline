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

# tests/ and src/ are siblings, not nested -- add src/ to the path
# explicitly so this resolves the same way regardless of where/how
# the script is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from load import get_spark_session


def verify_bronze(spark) -> None:
    print("=== Bronze ===")

    print("--- Row counts (expect 4,920 / 3,408) ---")
    spark.sql("SELECT COUNT(*) AS bronze_equities FROM financial_market_data.dev.bronze_equities").show()
    spark.sql("SELECT COUNT(*) AS bronze_treasury FROM financial_market_data.dev.bronze_treasury_yields").show()

    print("--- Duplicate key check, equities (expect zero rows) ---")
    spark.sql("""
        SELECT symbol, date, COUNT(*) c
        FROM financial_market_data.dev.bronze_equities
        GROUP BY symbol, date HAVING c > 1
    """).show()

    print("--- Duplicate key check, treasury (expect zero rows) ---")
    spark.sql("""
        SELECT series_id, date, COUNT(*) c
        FROM financial_market_data.dev.bronze_treasury_yields
        GROUP BY series_id, date HAVING c > 1
    """).show()

    print("--- OHLC internal consistency (expect zero rows: low <= open/close <= high) ---")
    spark.sql("""
        SELECT * FROM financial_market_data.dev.bronze_equities
        WHERE NOT (low <= open AND open <= high AND low <= close AND close <= high)
    """).show()

    print("--- Negative or zero prices/volume (expect zero rows) ---")
    spark.sql("""
        SELECT * FROM financial_market_data.dev.bronze_equities
        WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume <= 0
    """).show()

    print("--- Treasury null rate (expect ~0.0423 for both series) ---")
    spark.sql("""
        SELECT series_id,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_rate
        FROM financial_market_data.dev.bronze_treasury_yields
        GROUP BY series_id
    """).show()

    print("--- Schema check (symbol/date and series_id/date should show nullable = false,"
          " per load.py's BRONZE_SCHEMAS) ---")
    spark.table("financial_market_data.dev.bronze_equities").printSchema()
    spark.table("financial_market_data.dev.bronze_treasury_yields").printSchema()


def verify_silver(spark) -> None:
    print("=== Silver ===")

    print("--- Row count parity vs Bronze (expect 4,920 / 3,408, matching Bronze exactly) ---")
    spark.sql("SELECT COUNT(*) AS silver_equities FROM financial_market_data.dev.silver_equities").show()
    spark.sql("SELECT COUNT(*) AS silver_treasury FROM financial_market_data.dev.silver_treasury_yields").show()

    print("--- Duplicate key check, equities (expect zero rows) ---")
    spark.sql("""
        SELECT symbol, date, COUNT(*) c
        FROM financial_market_data.dev.silver_equities
        GROUP BY symbol, date HAVING c > 1
    """).show()

    print("--- Treasury null rate, unchanged from Bronze (expect ~0.0423 for both series) ---")
    spark.sql("""
        SELECT series_id,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_rate
        FROM financial_market_data.dev.silver_treasury_yields
        GROUP BY series_id
    """).show()

    print("--- Schema check (symbol/date and series_id/date should show nullable = false"
          " once sql/silver_constraints.sql has been run) ---")
    spark.table("financial_market_data.dev.silver_equities").printSchema()
    spark.table("financial_market_data.dev.silver_treasury_yields").printSchema()


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