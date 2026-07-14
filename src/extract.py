import os
import yaml
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

def load_sources_config(path="config/sources.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)["sources"]

def extract_equities(source_cfg):
    records = []
    for symbol in source_cfg["symbols"]:
        df = yf.download(
            symbol,
            start=source_cfg["start_date"],
            interval=source_cfg["interval"],
            progress=False,
        )
        # Recent yfinance versions return a MultiIndex column header
        # (e.g. ('Close', 'SPY')) even for a single-symbol download.
        # Flatten to plain column names before anything else touches them.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df["symbol"] = symbol
        records.append(df)
    combined = pd.concat(records, ignore_index=True)
    combined.columns = [c.lower() if isinstance(c, str) else c for c in combined.columns]
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    return combined

def extract_treasury_yields(source_cfg):
    api_key = os.environ["FRED_API_KEY"]
    records = []
    for series_id in source_cfg["series"]:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": source_cfg["start_date"],
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        obs = resp.json()["observations"]
        df = pd.DataFrame(obs)[["date", "value"]]
        df["series_id"] = series_id
        # FRED uses "." for missing observations
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        records.append(df)
    return pd.concat(records, ignore_index=True)

def run_extraction():
    sources = load_sources_config()
    results = {}
    for src in sources:
        if not src.get("enabled", True):
            continue
        if src["provider"] == "yfinance":
            results["equities"] = extract_equities(src)
        elif src["provider"] == "fred":
            results["treasury_yields"] = extract_treasury_yields(src)
    return results

if __name__ == "__main__":
    data = run_extraction()
    for name, df in data.items():
        print(f"{name}: {len(df)} rows extracted at {datetime.now(timezone.utc).isoformat()}")