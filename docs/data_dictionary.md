# Data Dictionary

_Financial Market Data Pipeline · Phase 6 · Last updated 2026-07-19_

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
- **Ad hoc Silver validation (post-transform, 2026-07-16):** Ran via `tests/verify.py`, which formalizes this check and the Bronze check above into one rerunnable script (`verify_bronze()` / `verify_silver()`), rather than the untracked manual spot-check the Bronze validation originally was. Checks: row count parity vs. Bronze, duplicate primary keys, treasury null rate, and Silver schema nullability. All passed: row counts match Bronze exactly (4,920 equities / 3,408 treasury — zero rows dropped), zero duplicate PKs, treasury null rate unchanged at 4.23% on both series, and `symbol`/`date` / `series_id`/`date` confirmed `nullable = false` after `sql/silver_constraints.sql` was applied. Now also validated on each Great Expectations run (`silver_treasury_yields_suite`) rather than only via the one-time manual spot-check.
- **Bronze nullability gap (found and resolved, 2026-07-16):** `load.py`'s `BRONZE_SCHEMAS` has always declared `symbol`/`date` and `series_id`/`date` as non-nullable, but that declaration only enforces types at write time — it does not tighten nullability on an already-existing Delta table, since Delta's `overwriteSchema=true` allows adding columns or widening types but not narrowing an existing column's nullability. As a result, both Bronze tables silently carried `nullable = true` on their key columns despite the code's declared intent, undetected because `verify_bronze()` didn't originally include a schema check. Found while building the equivalent Silver fix (`sql/silver_constraints.sql`) and generalized back to Bronze via `sql/bronze_constraints.sql`; `verify_bronze()` was extended with a schema check in the same pass to close the detection gap going forward. Confirmed resolved via live `printSchema()` — both tables now show `nullable = false` on their key columns.

---

## Dimension Layer

### `financial_market_data.dev.dim_securities`

- **Source:** `bronze_dim_securities_snapshot` (yfinance `Ticker.info`, extracted via `src/extract_reference.py`)
- **Grain:** One row per distinct version of a security's reference attributes — a security with N attribute changes has N+1 rows
- **Primary key:** `security_key` (random UUID, surrogate) — natural/business key is `symbol`, see `data_modeling_decisions.md` for the natural-key limitation and rationale
- **Row count:** 5 (3 current + 2 expired, as of 2026-07-17 — reflects one manufactured test transition on AAPL plus one genuine self-correction; see `data_modeling_decisions.md`)
- **Load strategy:** Append-only (SCD Type 2) — a deliberate exception to the Bronze/Silver full-replace convention; see `data_modeling_decisions.md` for full rationale.

| Column           | Type    | Nullable | Notes                                                                                                                  |
| ---------------- | ------- | -------- | ---------------------------------------------------------------------------------------------------------------------- |
| `security_key`   | string  | no       | Surrogate PK, random UUID — see `data_modeling_decisions.md`                                                           |
| `symbol`         | string  | no       | Natural/business key; limitation documented in `data_modeling_decisions.md`                                            |
| `company_name`   | string  | yes      | From yfinance `longName`/`shortName`                                                                                   |
| `sector`         | string  | yes      | `NULL` for ETFs (SPY, QQQ) by design — not a data quality issue                                                        |
| `exchange`       | string  | yes      | Yahoo's internal exchange code (e.g. `NMS`, `PCX`)                                                                     |
| `asset_type`     | string  | yes      | `EQUITY` / `ETF`, from yfinance `quoteType`                                                                            |
| `effective_date` | date    | yes      | When this version became active                                                                                        |
| `end_date`       | date    | yes      | `NULL` if current; set to `effective_date - 1` of the superseding version on expiry — see `data_modeling_decisions.md` |
| `is_current`     | boolean | yes      | Exactly one `true` row per `symbol`, enforced via `tests/verify.py`'s `verify_dim_securities`                          |

**Notes:**

- SCD2 merge logic lives in `src/transform_dim_securities.py` (`apply_scd2_dim_securities`) — separate from `transform.py`, since this is a stateful dimension merge, not a stateless per-batch clean.
- Validated via a 3-run test sequence against the real extraction pipeline; two real bugs found and fixed during testing (null-safe comparison logic, surrogate key collision) — full account in `data_modeling_decisions.md`.
- Regression-checked via `tests/verify.py`'s `verify_dim_securities()`: asserts exactly one current row per symbol, no symbol missing a current row, no expired row with a null `end_date`, no current row with a non-null `end_date`.

---

### `financial_market_data.dev.bronze_dim_securities_snapshot`

- **Source:** yfinance `Ticker.info`, one snapshot per run
- **Grain:** One row per symbol, as of the most recent extraction
- **Primary key:** `symbol` — no surrogate key at this layer; this is a raw landing snapshot, not a modeled dimension
- **Row count:** 3 (matches `config/sources.yml`'s equities symbol list)
- **Load strategy:** Full-replace (`overwrite`), consistent with Bronze convention — see `data_modeling_decisions.md`. `write_bronze()`'s `check_write_size()` guard is called with `min_write_ratio=0.99` here rather than the default `0.5`, since this table's correct row count is known exactly from `sources.yml`, not estimated from trend.

| Column          | Type   | Nullable | Notes                                              |
| --------------- | ------ | -------- | -------------------------------------------------- |
| `symbol`        | string | no       |                                                    |
| `company_name`  | string | yes      |                                                    |
| `sector`        | string | yes      | `NULL` for ETFs by design                          |
| `exchange`      | string | yes      |                                                    |
| `asset_type`    | string | yes      | `EQUITY` / `ETF`                                   |
| `snapshot_date` | date   | yes      | Date of extraction, not a trading/observation date |

**Notes:**

- Extraction logic in `src/extract_reference.py` — separate from `extract.py`, since this pulls point-in-time reference attributes, not recurring price/yield time series.
- This is the **input** to `dim_securities`'s SCD2 merge, not itself a modeled dimension — no `is_current`/`effective_date` logic at this layer.

---

## Gold Layer

### `financial_market_data.dev.fct_daily_returns`

- **Source:** `stg_equities` (dbt staging model, 1:1 pass-through of `silver_equities`)
- **Grain:** One row per symbol + trading date
- **Primary key:** `(symbol, date)` — enforced via dbt `not_null` tests on both columns; no composite uniqueness test (would require the `dbt_utils` package for a clean implementation — skipped as a new dependency not worth adding for one test, per `data_modeling_decisions.md`)
- **Row count:** 4,920 — matches Silver exactly (3 symbols × 1,640 trading days)
- **Load strategy:** dbt `table` materialization — full rebuild on every `dbt run`, not incremental

| Column         | Type   | Nullable | Notes                                                                                                                                                                                     |
| -------------- | ------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `symbol`       | string | no       | Enforced via dbt `not_null` test                                                                                                                                                          |
| `date`         | date   | no       | Enforced via dbt `not_null` test                                                                                                                                                          |
| `close`        | double | yes      | Passthrough from `silver_equities`                                                                                                                                                        |
| `prior_close`  | double | yes      | `LAG(close)` partitioned by `symbol`, ordered by `date`; null on each symbol's first trading day                                                                                          |
| `daily_return` | double | yes      | `(close - prior_close) / prior_close`; null on each symbol's first trading day by design — deliberately excluded from the dbt `not_null` test on this column, not backfilled or defaulted |

**Notes:**

- Exactly 1 null `daily_return` per symbol (3 total), confirmed via direct query and via Great Expectations' `fct_daily_returns_suite` — matches the expected first-trading-day-has-no-prior-close behavior exactly.
- Covered by Great Expectations (`fct_daily_returns_suite`): row count = 4,920, `daily_return` between -0.5 and 0.5 for at least 99% of rows (tolerates rare legitimate extreme-move days without hard-failing the suite on one outlier). See `docs/screenshots/great_expectations_report.png`.
- dbt lineage: `docs/screenshots/dbt_lineage.png`.

---

### `financial_market_data.dev.fct_market_yield_daily`

- **Source:** LEFT JOIN of `fct_daily_returns` against `stg_treasury_yields`, filtered to `series_id = 'DGS10'`
- **Grain:** One row per symbol + trading date — same grain as `fct_daily_returns`, extended with the matching 10-year Treasury yield
- **Primary key:** `(symbol, date)` — same enforcement approach as `fct_daily_returns`
- **Row count:** 4,920 — exact parity with `fct_daily_returns`, confirming the LEFT JOIN dropped no rows
- **Load strategy:** dbt `table` materialization — full rebuild on every `dbt run`, not incremental

| Column                      | Type   | Nullable | Notes                                                                                              |
| --------------------------- | ------ | -------- | -------------------------------------------------------------------------------------------------- |
| `symbol`                    | string | no       | Enforced via dbt `not_null` test                                                                   |
| `date`                      | date   | no       | Enforced via dbt `not_null` test                                                                   |
| `daily_return`              | double | yes      | Passthrough from `fct_daily_returns`                                                               |
| `treasury_10y_yield`        | double | yes      | 10-year Treasury yield (`DGS10`) for the same date; null when no matching yield observation exists |
| `treasury_10y_yield_change` | double | yes      | Day-over-day change in `treasury_10y_yield`                                                        |

**Notes:**

- LEFT JOIN chosen over INNER specifically to preserve every equity return row even without a matching yield date — verified via the exact row-count parity above, not assumed.
- `treasury_10y_yield` is null for 36 of 4,920 rows (0.73%), confirmed via Great Expectations' `fct_market_yield_daily_suite`. This is expected, not a defect: FRED does not publish DGS10 observations on days the bond market is closed (weekends, federal holidays), and that gap propagates through Silver and into this join rather than being interpolated. Consistent with `silver_treasury_yields`' own documented null rate above (4.23%) — the smaller percentage here reflects that equity trading days and bond-market closures don't align one-to-one.
- Covered by Great Expectations (`fct_market_yield_daily_suite`): row count = 4,920, `treasury_10y_yield` between 0 and 20. See `docs/screenshots/great_expectations_report.png`.

---

### `financial_market_data.dev.dim_securities_current`

- **Source:** `stg_dim_securities` (current-state slice of `dim_securities`, filtered to `is_current = true`)
- **Grain:** One row per symbol — current-state only, no history
- **Primary key:** `symbol` — enforced via dbt `unique` + `not_null` tests; `security_key` also enforced `unique` + `not_null`
- **Row count:** 3 (SPY, QQQ, AAPL)
- **Load strategy:** dbt `table` materialization — full rebuild on every `dbt run`, not incremental. Historical (non-current) versions remain queryable directly against `dim_securities` for point-in-time needs; this table intentionally does not expose them.

| Column           | Type    | Nullable | Notes                                                                                                                                                                                             |
| ---------------- | ------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `security_key`   | string  | no       | Passthrough from `dim_securities`; enforced `unique` + `not_null` via dbt                                                                                                                         |
| `symbol`         | string  | no       | Enforced `unique` + `not_null` via dbt                                                                                                                                                            |
| `company_name`   | string  | yes      | Passthrough from `dim_securities`                                                                                                                                                                 |
| `sector`         | string  | yes      | `NULL` for ETFs (SPY, QQQ) by design — not a data quality issue                                                                                                                                   |
| `exchange`       | string  | yes      | Passthrough from `dim_securities`                                                                                                                                                                 |
| `asset_type`     | string  | yes      | `EQUITY` / `ETF`; enforced via dbt `accepted_values` test — confirmed actual values are uppercase (`EQUITY`/`ETF`), not the initially-guessed lowercase, via direct query before writing the test |
| `effective_date` | date    | yes      | Passthrough from `dim_securities`                                                                                                                                                                 |
| `end_date`       | date    | yes      | `NULL` for all rows in this table by construction (current-state only)                                                                                                                            |
| `is_current`     | boolean | yes      | `true` for all rows in this table by construction (filter condition)                                                                                                                              |

**Notes:**

- Referential integrity enforced via dbt `relationships` test: every `fct_daily_returns.symbol` must exist in `dim_securities_current.symbol`.
- Covered by Great Expectations (`dim_securities_current_suite`): row count = 3, `asset_type` in `[EQUITY, ETF]`, `symbol` unique. See `docs/screenshots/great_expectations_silver_equities_suite.png` for an example of how GE renders a suite's full rule set (this table's suite follows the same format).

---

## Audit Layer

### `financial_market_data.dev.correction_log`

- **Source:** Derived — written by `src/apply_corrections.py` (`detect_and_apply_corrections`), not extracted from an upstream API
- **Grain:** One row per corrected field, per (symbol, date), per correction run — not one row per corrected record
- **Primary key:** None enforced; append-only audit trail, not a queryable dimension
- **Row count:** 5 (as of 2026-07-17, from one genuine vendor revision — see Notes)
- **Load strategy:** Append-only — every detected correction is logged permanently, never overwritten.

| Column          | Type      | Nullable | Notes                                                                          |
| --------------- | --------- | -------- | ------------------------------------------------------------------------------ |
| `symbol`        | string    | no       |                                                                                |
| `date`          | date      | no       | The historical trading date whose value was corrected, not the correction date |
| `field_changed` | string    | no       | One of `open`, `high`, `low`, `close`, `volume`                                |
| `old_value`     | string    | yes      | Cast to string — source columns span multiple types (double, long)             |
| `new_value`     | string    | yes      | Cast to string, same reason as `old_value`                                     |
| `corrected_at`  | timestamp | no       | When the correction was detected and applied, UTC                              |

**Notes:**

- Correction detection is windowed (trailing 30 days from run date), not full-history — see `data_modeling_decisions.md` for rationale.
- `old_value`/`new_value` are cast to string rather than kept in their native types, since a single log table spans corrections to `double` (`open`/`high`/`low`/`close`) and `long` (`volume`) columns.
- First live run (2026-07-17) detected a genuine yfinance revision to `volume` (AAPL, QQQ, SPY) and `high` (QQQ, SPY) for 2026-07-14, three trading days prior — not a synthetic test artifact. Full account, including why a synthetic mutation test was deemed unnecessary given this result, in `data_modeling_decisions.md`.
