# src/extract_reference.py
# Separate from extract.py: this pulls point-in-time security reference
# attributes (for dim_securities SCD2), not recurring price/yield time series.

import yfinance as yf
from datetime import date
import pandas as pd
import yaml
from extract import load_sources_config

def load_equities_symbols() -> list[str]:
    sources = load_sources_config()
    return next(s["symbols"] for s in sources if s["name"] == "equities")

def extract_security_snapshot(symbols: list[str]) -> pd.DataFrame:
    """Pull current reference attributes for each symbol.
    Returns one row per symbol as of today — caller is responsible
    for SCD2 comparison against existing dim_securities state.
    """
    rows = []
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            info = ticker.info  # single API call per symbol; info dict is cached by yfinance internally

            rows.append({
                "symbol": sym,
                "company_name": info.get("longName") or info.get("shortName"),
                "sector": info.get("sector"),  # None for ETFs — expected, not a bug
                "exchange": info.get("exchange"),
                "asset_type": info.get("quoteType"),  # 'EQUITY' vs 'ETF' from source
                "snapshot_date": date.today().isoformat(),
            })
        except Exception as e:
            print(f"WARNING: failed to fetch info for {sym}: {e}")

    return pd.DataFrame(rows)

if __name__ == "__main__":
    from load import get_spark_session, write_bronze

    symbols = load_equities_symbols()

    spark = get_spark_session()
    try:
        df = extract_security_snapshot(symbols)
        result = write_bronze(
            spark, df, "dim_securities_snapshot", min_write_ratio=0.99
        )
        print(result)
    finally:
        spark.stop()