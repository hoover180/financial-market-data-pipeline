import os
import time
import random
import logging
import yaml
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("extract")

# 429 and 5xx are transient — retrying helps. Other 4xx (bad request, bad
# API key, not found) won't resolve on retry, so fail fast instead of
# wasting the retry budget on an outcome that can't change.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable_http_error(exc):
    response = getattr(exc, "response", None)
    if response is None:
        return True  # connection/timeout errors carry no response object
    return response.status_code in RETRYABLE_STATUS_CODES


def with_retry(max_attempts=4, base_delay_seconds=2, max_delay_seconds=30):
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = base_delay_seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as exc:
                    if not _is_retryable_http_error(exc) or attempt == max_attempts:
                        raise
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout) as exc:
                    if attempt == max_attempts:
                        raise
                sleep_for = min(delay, max_delay_seconds) + random.uniform(0, 1)
                logger.warning(
                    "%s attempt %d/%d failed, retrying in %.1fs",
                    func.__name__, attempt, max_attempts, sleep_for,
                )
                time.sleep(sleep_for)
                delay *= 2
        return wrapper
    return decorator


def load_sources_config(path="config/sources.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)["sources"]


# yfinance fails silently (empty DataFrame) rather than raising on
# rate-limit/transient errors, so this checks the returned data itself
# rather than relying on exceptions the way with_retry does for FRED.
def _download_symbol_with_retry(symbol, source_cfg, max_attempts=4,
                                  base_delay_seconds=2, max_delay_seconds=30):
    delay = base_delay_seconds
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            df = yf.download(
                symbol,
                start=source_cfg["start_date"],
                interval=source_cfg["interval"],
                progress=False,
            )
        except Exception as exc:
            last_exc = exc
            df = None
        else:
            if df is not None and not df.empty:
                return df

        if attempt == max_attempts:
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(
                f"yfinance returned no data for {symbol} after {max_attempts} attempts"
            )

        sleep_for = min(delay, max_delay_seconds) + random.uniform(0, 1)
        logger.warning(
            "yfinance download for %s attempt %d/%d returned no data or raised, "
            "retrying in %.1fs", symbol, attempt, max_attempts, sleep_for,
        )
        time.sleep(sleep_for)
        delay *= 2


def extract_equities(source_cfg):
    records = []
    for symbol in source_cfg["symbols"]:
        df = _download_symbol_with_retry(symbol, source_cfg)
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


@with_retry()
def _fetch_fred_series(url, params):
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()["observations"]


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
        obs = _fetch_fred_series(url, params)
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