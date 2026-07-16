# Data Dictionary

_Financial Market Data Pipeline · Phase 3A · Last updated 2026-07-16_

---

## Bronze Layer

### `financial_market_data.dev.bronze_equities`

- **Source:** yfinance API
- **Grain:** One row per symbol + trading date
- **Primary key:** `(symbol, date)` — enforced `NOT NULL` at the Delta table level via a one-time `sql/bronze_constraints.sql`
- **Row count:** 4,920 (1,640 rows each for SPY, QQQ, AAPL — evenly balanced across all three symbols)
- **Date range:** 2020-01-02 → 2026-07-14 (matches configured `start_date` in `config/sources.yml`; end date reflects most recent trading day at ingest time)
- **Load strategy:** Full-replace (`overwrite`) — see `data_modeling_decisions.md` for full rationale.

| Column   | Type   | Nullable | Notes                                                                     |
| -------- | ------ | -------- | ------------------------------------------------------------------------- |
| `symbol` | string | no       | Ticker symbol (SPY, QQQ, AAPL); enforced via `sql/bronze_constraints.sql` |
| `date`   | date   | no       | Trading date; enforced via `sql/bronze_constraints.sql`                   |
| `open`   | double | yes      | Opening price                                                             |
| `high`   | double | yes      | Intraday high                                                             |
| `low`    | double | yes      | Intraday low                                                              |
| `close`  | double | yes      | Closing price (adjusted — see note)                                       |
| `volume` | long   | yes      | Shares traded                                                             |

**Notes:**

- yfinance serves _adjusted_ historical prices — values for a given historical date can change between ingest runs as splits/dividends are retroactively applied. This is the primary driver behind the full-replace load strategy — see `data_modeling_decisions.md` for full rationale.
- Source `yf.download()` returns a MultiIndex column header even for single-symbol requests; flattened to plain column names in `extract.py` before further processing.
- `symbol`/`date` non-nullability is enforced structurally at the Delta table level (`sql/bronze_constraints.sql`, `ALTER TABLE ... SET NOT NULL`) — not by `load.py`'s `BRONZE_SCHEMAS` alone. `BRONZE_SCHEMAS`'s explicit `StructType` enforces types on write, but does not tighten nullability on an already-existing Delta table. Confirmed via live `printSchema()` on 2026-07-16 — see Known Data Quality Notes below for the full story.

---

### `financial_market_data.dev.bronze_treasury_yields`

- **Source:** FRED API (Federal Reserve Bank of St. Louis)
- **Grain:** One row per series + date
- **Primary key:** `(series_id, date)` — enforced `NOT NULL` at the Delta table level via a one-time `sql/bronze_constraints.sql`
- **Row count:** 3,408 (1,704 rows each for DGS10, DGS2 — evenly balanced)
- **Date range:** 2020-01-01 → 2026-07-13
- **Load strategy:** Full-replace (`overwrite`) — see `data_modeling_decisions.md` for full rationale.

| Column      | Type   | Nullable | Notes                                                                                                                                                                    |
| ----------- | ------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `series_id` | string | no       | FRED series identifier (DGS10 = 10-Year Treasury, DGS2 = 2-Year Treasury); enforced via `sql/bronze_constraints.sql`                                                     |
| `date`      | date   | no       | Observation date; enforced via `sql/bronze_constraints.sql`                                                                                                              |
| `value`     | double | yes      | Yield value; **null on non-trading days** (holidays, weekends) rather than dropped — confirmed present in sample data (e.g. `DGS10, 2020-01-01, NULL`, a market holiday) |

**Notes:**

- FRED represents missing observations as `"."` in the raw API response; coerced to `None`/null in `extract.py` rather than silently dropping the row, so the calendar stays continuous and gaps are queryable/countable downstream.
- FRED republishes revised economic data after initial release — same rationale as equities for the full-replace strategy — see `data_modeling_decisions.md` for full rationale.
- `series_id`/`date` non-nullability is enforced structurally at the Delta table level (`sql/bronze_constraints.sql`) — same mechanism and confirmation as equities, above.

---

## Silver Layer

### `financial_market_data.dev.silver_equities`

- **Source:** `bronze_equities` (passthrough — type casts, ticker uppercasing, defensive deduplication only; no data-quality remediation, Bronze is already clean)
- **Grain:** One row per symbol + trading date
- **Primary key:** `(symbol, date)` — enforced `NOT NULL` at the Delta table level via a one-time `sql/silver_constraints.sql`, not per-run in `transform.py`
- **Row count:** 4,920 — matches Bronze exactly, no rows dropped
- **Load strategy:** Full-replace (`overwrite`), inherited from Bronze — see `data_modeling_decisions.md` for full rationale.

| Column   | Type   | Nullable | Notes                                                 |
| -------- | ------ | -------- | ----------------------------------------------------- |
| `symbol` | string | no       | Uppercased; enforced via `sql/silver_constraints.sql` |
| `date`   | date   | no       | Enforced via `sql/silver_constraints.sql`             |
| `open`   | double | yes      | Passthrough from Bronze                               |
| `high`   | double | yes      | Passthrough from Bronze                               |
| `low`    | double | yes      | Passthrough from Bronze                               |
| `close`  | double | yes      | Passthrough from Bronze                               |
| `volume` | long   | yes      | Passthrough from Bronze                               |

**Notes:**

- `symbol`/`date` non-nullability is enforced structurally at the Delta table level (`sql/silver_constraints.sql`, `ALTER TABLE ... SET NOT NULL`), run once after the table's first creation — not re-derived on every `transform.py` run. This applies to any future writer to the table, not just this script. `transform.py`'s `check_not_null()` is a cheap pre-write guard for a clear error message only; it is not the enforcement mechanism.

---

### `financial_market_data.dev.silver_treasury_yields`

- **Source:** `bronze_treasury_yields` (passthrough — type casts, series_id uppercasing, defensive deduplication only)
- **Grain:** One row per series + date
- **Primary key:** `(series_id, date)` — enforced `NOT NULL` at the Delta table level via a one-time `sql/silver_constraints.sql`
- **Row count:** 3,408 — matches Bronze exactly, no rows dropped
- **Load strategy:** Full-replace (`overwrite`), inherited from Bronze — see `data_modeling_decisions.md` for full rationale.

| Column      | Type   | Nullable | Notes                                                                                                     |
| ----------- | ------ | -------- | --------------------------------------------------------------------------------------------------------- |
| `series_id` | string | no       | Uppercased; enforced via `sql/silver_constraints.sql`                                                     |
| `date`      | date   | no       | Enforced via `sql/silver_constraints.sql`                                                                 |
| `value`     | double | yes      | Nulls (~4.23%, holiday-driven) intentionally preserved — not dropped or imputed, see Bronze section above |

**Notes:**

- Nulls in `value` are passed through unchanged from Bronze — this column is deliberately excluded from `sql/silver_constraints.sql`'s `NOT NULL` columns.
- Same enforcement-mechanism note as `silver_equities` above: constraint lives at the table level, not in `transform.py`.

---

## Known Data Quality Notes

- **Validation false-positive (resolved):** Initial validation runs flagged 100% of equities rows (4,920/4,920) as schema-invalid. Root cause: `validate.py` originally used `df.iterrows()`, which preserves numpy scalar types (e.g. `numpy.int64` for `volume`) rather than converting to native Python types — `numpy.int64` fails a JSON Schema `"type": "integer"` check even though the value is a true integer. Fixed by switching to `df.to_dict(orient="records")`, which pandas boxes to native Python types automatically. Additionally, `date` was being validated as a pandas `Timestamp` object against a `"type": "string"` schema; resolved by explicitly formatting to `YYYY-MM-DD` string in `extract.py` before validation.
- **Schema provisioning:** `financial_market_data.dev` schema does not yet exist automatically — provisioned manually via `CREATE SCHEMA IF NOT EXISTS` ahead of the Phase 2 run. Will move to Terraform-managed provisioning in Phase 7, consistent with the rest of the pipeline's infrastructure-as-code approach.
- **Ad hoc Bronze validation (post-load, 2026-07-14):** Ran manual checks for duplicate primary keys, OHLC internal consistency (low ≤ open/close ≤ high), negative/zero prices, treasury null rate, and date-gap continuity. All passed: zero duplicate PKs, zero OHLC violations, zero negative/zero values, treasury null rate 4.23% on both series (consistent with federal holiday coverage — FRED's daily series already excludes weekends), zero gaps over 5 days in equities trading dates. This was a manual spot-check, not the formal Great Expectations framework — that's Phase 6. Formalized into a rerunnable script in Phase 3A — see below.
- **Ad hoc Silver validation (post-transform, 2026-07-16):** Ran via `tests/verify.py`, which formalizes this check and the Bronze check above into one rerunnable script (`verify_bronze()` / `verify_silver()`), rather than the untracked manual spot-check the Bronze validation originally was. Checks: row count parity vs. Bronze, duplicate primary keys, treasury null rate, and Silver schema nullability. All passed: row counts match Bronze exactly (4,920 equities / 3,408 treasury — zero rows dropped), zero duplicate PKs, treasury null rate unchanged at 4.23% on both series, and `symbol`/`date` / `series_id`/`date` confirmed `nullable = false` after `sql/silver_constraints.sql` was applied. Still a manual spot-check, not the formal Great Expectations framework — that's Phase 6.
- **Bronze nullability gap (found and resolved, 2026-07-16):** `load.py`'s `BRONZE_SCHEMAS` has always declared `symbol`/`date` and `series_id`/`date` as non-nullable, but that declaration only enforces types at write time — it does not tighten nullability on an already-existing Delta table, since Delta's `overwriteSchema=true` allows adding columns or widening types but not narrowing an existing column's nullability. As a result, both Bronze tables silently carried `nullable = true` on their key columns despite the code's declared intent, undetected because `verify_bronze()` didn't originally include a schema check. Found while building the equivalent Silver fix (`sql/silver_constraints.sql`) and generalized back to Bronze via `sql/bronze_constraints.sql`; `verify_bronze()` was extended with a schema check in the same pass to close the detection gap going forward. Confirmed resolved via live `printSchema()` — both tables now show `nullable = false` on their key columns.
