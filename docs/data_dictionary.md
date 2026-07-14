# Data Dictionary — Bronze Layer

_Financial Market Data Pipeline · Phase 2 · Last updated 2026-07-14_

---

## `financial_market_data.dev.bronze_equities`

- **Source:** yfinance API
- **Grain:** One row per symbol + trading date
- **Primary key:** `(symbol, date)`
- **Row count:** 4,920 (1,640 rows each for SPY, QQQ, AAPL — evenly balanced across all three symbols)
- **Date range:** 2020-01-02 → 2026-07-14 (matches configured `start_date` in `config/sources.yml`; end date reflects most recent trading day at ingest time)
- **Load strategy:** Full-replace (`overwrite`) — see rationale below.

| Column   | Type   | Nullable | Notes                               |
| -------- | ------ | -------- | ----------------------------------- |
| `symbol` | string | yes      | Ticker symbol (SPY, QQQ, AAPL)      |
| `date`   | date   | yes      | Trading date                        |
| `open`   | double | yes      | Opening price                       |
| `high`   | double | yes      | Intraday high                       |
| `low`    | double | yes      | Intraday low                        |
| `close`  | double | yes      | Closing price (adjusted — see note) |
| `volume` | long   | yes      | Shares traded                       |

**Notes:**

- yfinance serves _adjusted_ historical prices — values for a given historical date can change between ingest runs as splits/dividends are retroactively applied. This is the primary driver behind the full-replace load strategy (see below).
- Source `yf.download()` returns a MultiIndex column header even for single-symbol requests; flattened to plain column names in `extract.py` before further processing.

---

## `financial_market_data.dev.bronze_treasury_yields`

- **Source:** FRED API (Federal Reserve Bank of St. Louis)
- **Grain:** One row per series + date
- **Primary key:** `(series_id, date)`
- **Row count:** 3,408 (1,704 rows each for DGS10, DGS2 — evenly balanced)
- **Date range:** 2020-01-01 → 2026-07-13
- **Load strategy:** Full-replace (`overwrite`) — see rationale below.

| Column      | Type   | Nullable | Notes                                                                                                                                                                    |
| ----------- | ------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `series_id` | string | yes      | FRED series identifier (DGS10 = 10-Year Treasury, DGS2 = 2-Year Treasury)                                                                                                |
| `date`      | date   | yes      | Observation date                                                                                                                                                         |
| `value`     | double | yes      | Yield value; **null on non-trading days** (holidays, weekends) rather than dropped — confirmed present in sample data (e.g. `DGS10, 2020-01-01, NULL`, a market holiday) |

**Notes:**

- FRED represents missing observations as `"."` in the raw API response; coerced to `None`/null in `extract.py` rather than silently dropping the row, so the calendar stays continuous and gaps are queryable/countable downstream.
- FRED republishes revised economic data after initial release — same rationale as equities for the full-replace strategy.

---

## Bronze Load Strategy: Full-Replace (`overwrite`)

Both sources are subject to retroactive revision:

- yfinance adjusts historical closing prices for splits and dividends after the fact.
- FRED republishes revised economic data.

An append-only Bronze layer would lock in stale/incorrect historical values from earlier runs and never correct them. Full-replace avoids this at the cost of no run-to-run audit history at the Bronze layer.

**Production alternative (not implemented, scope excluded):** a production system facing this trade-off would typically snapshot raw API responses to immutable storage _before_ any transformation, preserving a true audit trail independent of the queryable Bronze table — decoupling "what did the source say on date X" from "what do we currently believe is true for date X."

---

## Known Data Quality Notes

- **Validation false-positive (resolved):** Initial validation runs flagged 100% of equities rows (4,920/4,920) as schema-invalid. Root cause: `validate.py` originally used `df.iterrows()`, which preserves numpy scalar types (e.g. `numpy.int64` for `volume`) rather than converting to native Python types — `numpy.int64` fails a JSON Schema `"type": "integer"` check even though the value is a true integer. Fixed by switching to `df.to_dict(orient="records")`, which pandas boxes to native Python types automatically. Additionally, `date` was being validated as a pandas `Timestamp` object against a `"type": "string"` schema; resolved by explicitly formatting to `YYYY-MM-DD` string in `extract.py` before validation.
- **Schema provisioning:** `financial_market_data.dev` schema does not yet exist automatically — provisioned manually via `CREATE SCHEMA IF NOT EXISTS` ahead of the Phase 2 run. Will move to Terraform-managed provisioning in Phase 7, consistent with the rest of the pipeline's infrastructure-as-code approach.
- **Ad hoc Bronze validation (post-load, 2026-07-14):** Ran manual checks for duplicate primary keys, OHLC internal consistency (low ≤ open/close ≤ high), negative/zero prices, treasury null rate, and date-gap continuity. All passed: zero duplicate PKs, zero OHLC violations, zero negative/zero values, treasury null rate 4.23% on both series (consistent with federal holiday coverage — FRED's daily series already excludes weekends), zero gaps over 5 days in equities trading dates. This was a manual spot-check, not the formal Great Expectations framework — that's Phase 6.
