# Data Modeling Decisions

> This document records every significant modeling choice made in this project, with explicit rationale. Intended as both a project record and an interview reference.

---

## Load Strategy: Full-Replace (`overwrite`) — Bronze and Silver

**Decision:** Both Bronze and Silver use `overwrite` (full-replace) rather than incremental append or MERGE. Silver inherits the pattern from Bronze rather than choosing independently.

**Design principle:** Bronze and Silver are modeled as current-state layers, not historical archives. Correctness with respect to the latest upstream publication is prioritized over preserving every intermediate revision.

**Why Bronze is full-replace:**

Both source APIs revise historical values after initial publication:

- yfinance serves _adjusted_ historical prices — values for a given historical date change between ingest runs as stock splits and dividends are retroactively applied.
- FRED republishes revised economic data after initial release.

An append-only Bronze layer would lock in stale/incorrect historical values from earlier runs and never correct them, since there'd be no mechanism to distinguish a value that needs updating from one that's final. Full-replace avoids this by construction — every run reflects the source's current understanding of history, not a frozen first-seen snapshot.

Because Bronze intentionally represents the latest published state rather than an immutable landing zone, it serves as a reproducible staging layer for downstream processing rather than as a historical archive — a deliberate deviation from the classic append-only-Bronze pattern, made explicitly for these two sources rather than adopted as a default.

**Why Silver inherits it rather than choosing independently:**

An incremental or merge-based Silver layer would be conforming a mixture of records reflecting different upstream revisions with no reliable way to tell them apart, since Bronze itself doesn't preserve that distinction. Keeping Silver as full-replace ensures it's always consistent with the _latest_ Bronze snapshot rather than a blend of old and new source-of-truth states. This is a deliberate choice, not a placeholder or default: incremental/MERGE logic is introduced intentionally in Phase 3B for the historical-dimension work (SCD Type 2), where it serves a real purpose — preserving history _within the pipeline's own modeling_, rather than being applied here where it would add complexity while inheriting Bronze's already-ambiguous freshness problem.

**Delta Lake time travel — a partial mitigation, not a substitute for a real audit trail:**

Full-replace isn't a total loss of history at the storage layer. Delta Lake's transaction log versions every `overwrite`, so `VERSION AS OF` / `TIMESTAMP AS OF` time travel can recover recent prior states of either table without any extra engineering.

That said, time travel falls short of a true audit trail for three reasons:

1. **Bounded retention** — 30-day log retention and 7-day `VACUUM` retention by default, so it only covers a recent window unless retention is explicitly extended.
2. **Post-transformation only** — it captures the table's state _after_ `extract.py` (Bronze) or the cleaning logic (Silver) has run, so a bug in that logic is baked into every historical version equally; it can't show what the raw API actually returned.
3. **Delta-coupled** — not portable if the pipeline ever moves off Delta as the storage engine.

**Production alternative (not implemented, scope excluded):** a production system facing this trade-off would typically snapshot raw API responses into immutable object storage _before_ any transformation, independent of Delta's versioning — decoupling "what did the source say on date X" from "what do we currently believe is true for date X," and surviving both retention limits and any future changes to the transformation logic or storage engine.

**Alternative considered and deferred: change-detection MERGE.** A more surgical approach — `MERGE INTO` comparing each incoming row against the existing row on the same key, writing only rows that are new or whose values actually changed — was considered as a more scalable replacement for full-replace. This is a legitimate pattern (Delta's `MERGE INTO` is purpose-built for it) and would give smaller, more meaningful per-run diffs, lower write volume at scale, and near-free change tracking via a `last_revised_date` column.

Two things led to deferring it rather than implementing it:

1. **Split-adjustment history-wide revision problem.** yfinance's adjusted close isn't a "sometimes one cell changes" case — a stock split retroactively re-adjusts the _entire_ historical series back to inception. A MERGE can't safely assume only recent rows might have changed and limit its comparison window; it would need to diff against the full table on every run to reliably catch a split-driven revision anywhere in history.

   That significantly reduces the potential read-side savings compared with datasets where revisions are naturally localized — though Delta's partition pruning and data skipping still provide some benefit even under a full comparison; this isn't a naive table scan. The write-side savings — only writing rows that actually differ — still hold regardless.

2. **Negligible payoff at current scale.** At 4,920 / 3,408 rows, a full-table `overwrite` completes in seconds on serverless compute. The added implementation surface — row-level float comparison with precision-mismatch false positives, correct NULL-vs-value handling, and confidence that a partial comparison wouldn't silently miss a split-driven revision — isn't justified by a compute cost that isn't actually a problem yet.

FRED treasury yields are a better fit for this pattern than equities, since observed market rates aren't subject to the same retroactive, whole-series revision as split-adjusted prices — a MERGE against `treasury_yields` would likely show near-zero writes on re-runs in practice. This would be the right call to revisit at production scale, or if the pipeline ever ingests a source with genuinely large per-run data volume.

**Idempotency and deterministic rebuilds.** Because each run reconstructs Bronze and Silver from the current upstream truth rather than accumulating prior state, repeated executions against unchanged source data produce identical outputs — both layers are naturally idempotent. This also simplifies recovery: if either layer is ever corrupted by a bug or bad run, the fix isn't a targeted repair, it's a rerun. That operational simplicity is a direct, deliberate consequence of choosing full-replace, not an incidental side effect.

**Known risk: full-replace has no built-in defense against a bad extraction (mitigated 2026-07-16).** A malformed or empty result from `extract.py` — an API rate limit, a schema change, a transient failure — would overwrite good historical data with corrupted or empty data rather than leaving the prior good state intact, since `overwrite` doesn't distinguish "new good data" from "new bad data."

`load.py`'s `write_bronze()` now guards against this via `check_write_size()`: before committing the overwrite, it compares the incoming row count against the existing table's current row count and aborts with a `ValueError` if the new extraction falls below 50% of that (`min_write_ratio`, default `0.5`). No-op on the table's first write or if the existing table is empty, since there's nothing to compare against yet.

The 50% threshold is a deliberate judgment call, not derived from the data — generous enough to tolerate legitimate day-to-day variance (e.g. a holiday-shortened trading week) while still catching a genuinely partial or rate-limited extraction.

This guard has a real limitation, worth stating plainly: it only catches a _volume_ drop. A garbled extraction that happens to return the same row count with corrupted values (wrong prices, misaligned dates, etc.) passes this check silently — that class of error is out of scope for this guard and is deferred to the Great Expectations layer in Phase 6, which validates values, not just row counts.

**Trade-off accepted:** no indefinite run-to-run audit history at either layer, beyond Delta's bounded time-travel window. Acceptable for this project's scope.

---

## dim_securities — Slowly Changing Dimension (Type 2)

**Grain:** one row per distinct version of a security's reference attributes (`company_name`, `sector`, `exchange`, `asset_type`), keyed by `security_key`. A security with N attribute changes over its history has N+1 rows in this table.

**Decision:** `dim_securities` uses `symbol` as the natural/business key, with the limitation documented explicitly below, rather than sourcing a durable external identifier.

**Why `symbol` is not a fully durable natural key:** ticker symbols can change on corporate actions — rebrands (e.g. FB → META), delistings, mergers, exchange migrations. At production scale, the correct pattern is a durable external identifier (CIK, FIGI, or ISIN) as the true natural key, with `symbol` demoted to a tracked SCD2 attribute rather than the join key itself.

**Considered and rejected: ISIN via yfinance.** Tested `Ticker.get_isin()` against all three symbols in the project's universe. Individual equities returned valid ISINs (AAPL: `US0378331005`), but ETF coverage was inconsistent — SPY returned a valid ISIN, QQQ returned `'-'`, yfinance's no-data sentinel. `Ticker.info.get('isin')` returned `None` for all three and is not a usable fallback. Given the ETF gap in this specific data source, `isin` was not adopted as a dimension attribute. A production implementation would source a durable identifier from a proper reference-data provider (e.g. OpenFIGI, SEC CIK) rather than relying on Yahoo Finance's inconsistent ETF coverage.

**Why this is safely deferrable, not a hidden liability:** fact tables join to this dimension via the surrogate `security_key`, resolved from `symbol` at ETL time — they never carry `symbol` as a permanent foreign key. Migrating the natural-key source to a durable identifier in the future would be contained entirely to `dim_securities`'s key-generation logic and would not require changes to fact tables or historical joins. This is the surrogate-key pattern doing its job: today's key _source_ is swappable precisely because downstream consumers never depend on it directly.

**Scope note:** this project's fixed 3-symbol universe (SPY, QQQ, AAPL) carries negligible practical risk of a symbol change occurring within the project's timeframe. ETF ticker instability (sponsor mergers, product closures, rebrands) is real at the broader market level more often than for individual equities, but SPY and QQQ are decades-old flagship products for their sponsors, not the kind of product that gets rebranded away.

**Surrogate key — random UUID, not a deterministic hash.** `security_key` is generated via `F.expr("uuid()")`, a true surrogate key not derived from business data.

Worth keeping the design history on record: the original implementation used `sha2(symbol + effective_date)` as a deterministic hash. This seemed reasonable — idempotent, human-traceable — but was wrong: it collides whenever a symbol transitions more than once on the same calendar date, since the hash inputs would be identical. This was caught during testing, not by inspection (see "Testing and bugs found" below) — the collision was invisible on first read and only surfaced when a manufactured same-day transition produced two rows sharing one key. Switched to a random UUID, which guarantees uniqueness regardless of how many versions a symbol accumulates on a single day.

**`end_date` convention: `today - 1`, not `today`.** When a row is expired, `end_date` is set to `today - 1`, keeping the expired row's effective range (`[old_effective_date, today - 1]`) and the new row's range (`[today, NULL]`) disjoint. With `end_date = today`, both ranges would technically include "today," requiring any point-in-time query (`WHERE date BETWEEN effective_date AND end_date`) to add an `is_current` tiebreaker to resolve the ambiguity. The `today - 1` convention avoids that landmine for any future consumer of this table who doesn't know the rule.

**Load strategy: append-only — a deliberate exception to the Bronze/Silver full-replace convention documented above.** SCD2's entire purpose is accumulating history, so overwriting on every run would destroy the versioned record the pattern exists to preserve. First run writes with `overwriteSchema=true` (table doesn't exist yet); subsequent runs `append` new versions and `MERGE`-update only the `end_date`/`is_current` fields on rows being expired. This isn't a contradiction of the full-replace decision above — it's a different table serving a different purpose (versioned dimension vs. current-state fact staging), with the load strategy chosen deliberately per table rather than applied uniformly without consideration. This is exactly the incremental/MERGE work the Bronze/Silver section above anticipated when it deferred change-detection MERGE to "the historical-dimension work (SCD Type 2)."

**Testing and bugs found.** Validated via a three-run sequence against the real Bronze extraction pipeline, not synthetic seed data:

1. **Run 1** (first run): all 3 symbols inserted as v1 — `{'inserted': 3, 'expired': 0, 'unchanged': 0, 'first_run': True}`.
2. **Manual mutation:** AAPL's `sector` changed directly in `bronze_dim_securities_snapshot`, simulating a new snapshot arriving with a changed attribute.
3. **Run 2, first attempt:** expected AAPL to transition with SPY/QQQ as no-ops. Actual result: `{'inserted': 3, 'expired': 3, 'unchanged': 0}` — all three symbols incorrectly transitioned.
4. **Root cause:** the changed-attribute comparison used `col != col OR col.isNull()`, intended to catch "no current row exists yet" on a left join. In SQL, `NULL != NULL` evaluates to `NULL`, and `NULL OR True = True` — so any row with a genuinely null tracked attribute (`sector` is null for both ETFs by design) was always flagged as changed, regardless of whether it had actually changed. Fixed by switching to null-safe equality (`eqNullSafe`), which correctly treats `NULL <=> NULL` as unchanged.
5. **The same run also surfaced the `security_key` collision** described above — both AAPL rows landed with an identical key, since Run 1 and Run 2 executed on the same calendar date.
6. **Both fixes applied, sequence re-run from Run 1.** Final result after Run 3 (a fresh real extraction restoring AAPL's true `sector`): 5 total rows — SPY and QQQ stable across all three runs with unchanged `security_key`s throughout, AAPL showing a clean three-version history (`Technology` → `Consumer Discretionary` → `Technology`), exactly one `is_current = true` row per symbol, correct `end_date` on every expired row.
7. **Automated regression check** added to `tests/verify.py` (`verify_dim_securities`): asserts exactly one current row per symbol, no symbol missing a current row, no expired row with a null `end_date`, no current row with a non-null `end_date`.

Both bugs were real and would not have been caught by code review alone — the null-safe equality issue in particular only manifests when a tracked attribute is legitimately null, which isn't obvious from reading the comparison logic in isolation.

---

## Late-Arriving Corrections — `silver_equities`

**Framing: batch revision, not streaming out-of-order arrival.** For batch-pulled market data, "late-arriving" doesn't mean records arrive out of temporal order in a stream — there is no stream here. The real-world equivalent for this data type is a previously-loaded historical row getting revised by the source: yfinance backfilling a corrected `volume` or `high` figure as consolidated tape data settles, or a stock split retroactively adjusting historical prices (the same phenomenon driving the full-replace decision above). This project's correction-handling is built around that framing, not literal out-of-order timestamp handling, which would be a different and here-inapplicable problem.

**Decision: windowed detection, not full-history re-check.** `detect_and_apply_corrections()` re-pulls only a trailing window (default 30 days) of equities data and diffs it against the corresponding slice of `silver_equities`, rather than re-pulling and comparing full history on every run. Real-world vendor revisions/backfills are almost always recent — re-checking years of history for negligible additional coverage would be wasteful API usage. 30 days is a judgment call, not derived from a documented vendor SLA: generous enough to catch typical correction lag, cheap enough to run frequently. A production system might tune this window based on observed correction latency from the specific vendor(s) in use.

**Mechanics:**

- The fresh window pull is routed through the same `prepare_dataframe()` + `transform_equities()` path Bronze/Silver already use, not a separate casting implementation. This was deliberate: if the fresh pull and Silver were cast/typed through two different code paths, any diff found could be a formatting artifact rather than a genuine vendor revision, with no way to tell the two apart.
- Field comparison uses null-safe equality (`eqNullSafe`) per field — the same fix applied to `dim_securities`'s comparison logic, for the same reason.
- Every changed field is logged to `correction_log` as its own row (`symbol`, `date`, `field_changed`, `old_value`, `new_value`, `corrected_at`) rather than one coarse "this row changed" entry, giving a precise, queryable audit trail.
- The Delta `MERGE` writes the full corrected row — all 5 OHLCV fields from the fresh pull — not a partial patch of only the field(s) that individually differed. This avoids a scenario where two independent corrections landing across separate runs leave a row with a mix of stale and fresh values.
- Only rows already present in `silver_equities` within the window are eligible for correction (inner join). This function corrects existing history; it does not backfill missing dates — that remains the normal Bronze/Silver pipeline's responsibility.

**Load strategy: MERGE — a second, distinct exception to the full-replace convention documented above.** Like `dim_securities`, this departs from full-replace because a targeted correction to specific historical rows is a fundamentally different operation from replacing an entire table on each run. Unlike `dim_securities`, this MERGE overwrites values in place rather than appending a new version — corrections here are treated as fixing the current-state record, not as a versioned history to preserve. That's consistent with Silver's stated purpose as a current-state layer, not a historical archive.

**Validation: caught a real correction on first live run.** The baseline run — intended only to confirm zero corrections against a freshly-loaded table — instead detected and correctly applied a genuine vendor revision: yfinance's `volume` figures for AAPL, QQQ, and SPY, and `high` figures for QQQ and SPY, were revised for 2026-07-14 (three trading days prior) between initial load and this run, consistent with known consolidated-tape settling behavior. All five field-level changes were logged to `correction_log` with old/new values and correctly applied to `silver_equities` via MERGE. Verified by direct query against both the log and the corrected table.

This is stronger validation than a synthetic test would have provided — the detection logic proved itself against real, unstaged vendor behavior on its first production run, not fabricated input. A synthetic mutation-and-revert test was planned but deemed unnecessary: it would have re-exercised comparison/MERGE logic that is identical (same `eqNullSafe` condition, same `update_set` construction) regardless of which field changes, and would have required introducing and then cleaning up fabricated data in a table with no lasting artifact to show for it, since the mutation was never going to be checked into the codebase.

---

## DuckDB Local Validation Layer

**Decision:** Mirror all 7 Delta tables (Bronze, Silver, Dimension, Audit) into a local DuckDB file (`data/local_dev.duckdb`) via a full-replace refresh script (`src/mirror_to_duckdb.py`), rather than querying Databricks directly for every dev-loop iteration.

**Rationale:** Cost-conscious engineering — local validation avoids spinning up serverless SQL warehouse compute for every ad hoc query during active development. This is a read-only mirror, never a second source of truth; no writes ever flow back from DuckDB to Delta.

**Refresh pattern:** `CREATE OR REPLACE TABLE` per table — consistent with the project's existing full-replace convention for Bronze/Silver (fails fast on schema drift rather than silently accumulating stale state from a partial sync).

**Verification:** `tests/verify_duckdb_mirror.py` confirms row-count parity between the local mirror and source Delta tables after each mirror run.

---

## Gold Marts — Daily Returns & Treasury Yield Join

**Grain (`fct_daily_returns`):** One row per symbol per trading day. Daily return is calculated via `LAG(close)` partitioned by symbol, ordered by date. The first trading day per symbol has a null `daily_return` by design — no prior close exists to compare against — and is deliberately excluded from the `not_null` dbt test on that column rather than backfilled or defaulted.

**Join logic (`fct_market_yield_daily`):** LEFT JOIN of `fct_daily_returns` against `stg_treasury_yields`, filtered to `series_id = 'DGS10'` (10-year Treasury — confirmed as the correct series alongside `DGS2` via direct query against `silver_treasury_yields`, not assumed from `sources.yml` alone). LEFT JOIN was chosen over INNER to guarantee every equity return row is preserved even on a date with no matching yield record. Verified, not assumed: row counts match exactly across both marts (4,920 rows — 3 symbols × 1,640 trading days each).

---

## Deferred: Local Unit Testing for `transform.py`

**Constraint:** `transform.py`'s functions (`transform_equities`, `transform_treasury`, `check_not_null`) take a DataFrame in and return a DataFrame out — no direct Spark session calls inside them. Despite that, they aren't unit tested locally: this project's Spark access is via `databricks-connect`, which only supports remote sessions against the live Databricks warehouse, with no local/offline session available to build a test DataFrame.

**Alternative considered and rejected:** running these tests against the real warehouse. This was rejected as slow, dependent on live cloud auth, and costing compute per run — turning a unit test into an integration test in practice.

**Path forward, not pursued:** a separate test-only environment with plain, non-Databricks `pyspark` would restore local testing with no code changes required, since the functions are already structured to accept and return a DataFrame. Deferred as not worth the added maintenance at this project's current size; worth revisiting if transformation logic grows materially more complex.

**Correctness today** is covered by `tests/verify.py` (real-data checks against actual pipeline output) and `dbt test` (10 passing tests on the Gold marts).

---

## Great Expectations — Execution Engine and Scope

**Decision: SQL execution engine, not Spark.** Great Expectations' `SparkDFExecutionEngine` expects a local/classic Spark session. This project's Spark access is via `databricks-connect`, which only supports remote sessions against the live Databricks warehouse — the same constraint documented above under "Deferred: Local Unit Testing for `transform.py`." Rather than fight that constraint a second time, GE is configured with a SQL datasource (`add_or_update_databricks_sql`) that queries Unity Catalog tables directly via `databricks-sql-connector`, consistent with this project's established pattern of validating against real data over forcing a mismatched tool into an environment it wasn't built for.

**Decision: five consumer-facing tables, not all seven.** `silver_equities`, `silver_treasury_yields`, `fct_daily_returns`, `fct_market_yield_daily`, and `dim_securities_current` receive GE suites. `correction_log` and the raw `dim_securities` SCD2 table are excluded: `correction_log` is an audit trail with no downstream consumer, and `dim_securities`'s SCD2 integrity is already fully covered by `tests/verify.py`. Adding GE suites to either would duplicate existing coverage without adding signal.

**Decision: freshness and range checks, not duplicated uniqueness/not-null coverage.** `dbt test`'s 10 passing tests and `tests/verify.py` already cover uniqueness, not-null, and referential integrity. GE's distinct value here is freshness (max date within a rolling window — nothing else in the project checks staleness) and range/set-membership checks GE expresses more naturally than a dbt test (e.g., `expect_column_values_to_be_between` on `close`, `value`, `daily_return`, `treasury_10y_yield`). This scoping decision was made explicitly before writing checks, consistent with how every other phase in this project has approached tool selection.

**Validation artifact:** all five suites pass (`success: true`, 5/5 validations, 15/15 expectations) against live Databricks data. Data Docs screenshots saved to `docs/screenshots/great_expectations_report.png` (validation results summary) and `docs/screenshots/great_expectations_silver_equities_suite.png` (example suite definition, showing the full rule set as GE renders it).

**Decision: secrets via `gx/uncommitted/config_variables.yml`, not embedded in the connection string.** An initial attempt to reference `${DBT_DATABRICKS_TOKEN}` directly inside the Python-constructed connection string resulted in GE resolving and writing the literal token to the committed `great_expectations.yml` regardless — GE's Fluent API resolves the full connection string against the environment at datasource-creation time (to validate connectivity) and serializes the resolved result back to disk, not the original template. The documented mechanism is instead to define the credential in `config_variables.yml` (itself excluded from git by GE's default `.gitignore`, and itself sourcing `${DBT_DATABRICKS_TOKEN}` from the environment) and reference it via `${DATABRICKS_TOKEN}` in the connection string. Confirmed working via direct pattern search on the committed file (`dapi` absent, `${DATABRICKS_TOKEN}` present, unresolved) before committing.

---

## Terraform — Resource Scope and Provider Auth

**Decision: Databricks-native Unity Catalog resources, not cloud-provider resources (S3/IAM).** The original build plan's "storage bucket + IAM role/policy" framing assumed a general-purpose cloud account (AWS/Azure/GCP) alongside Databricks. This project doesn't have one, and Databricks Free Edition doesn't expose cluster/compute provisioning or account-level cloud infrastructure APIs. Rather than introduce a second cloud account and a new billing surface solely to satisfy a literal bucket-and-IAM-role checkbox, the same storage-plus-access-control pattern was mapped onto Unity Catalog's own native resource types via the `databricks` Terraform provider:

- `databricks_schema` — a governed Unity Catalog schema, functioning as the storage-equivalent resource (the container Delta tables live in, analogous to a bucket).
- `databricks_grants` — a `USE_SCHEMA` + `SELECT` grant on that schema, functioning as the IAM-equivalent resource (access control on the storage resource).

This keeps the two-resource scope from the build plan intact and stays entirely inside the existing OAuth-based `financial_market_data_pipeline` CLI profile already used elsewhere in this project — no new cloud account, no new auth surface, no new billing risk.

**Decision: local state, gitignored; provider auth via existing CLI profile.** State is kept local rather than in a remote backend (e.g., Terraform Cloud, an S3 backend) — for a two-resource demonstration, the operational overhead of a remote backend isn't justified by what it's protecting. `terraform.tfstate` and `.tfstate.backup` are gitignored alongside `.terraform/` and `*.tfvars`.

Provider auth uses the existing `financial_market_data_pipeline` CLI profile — the same mechanism `databricks-connect` already uses elsewhere in this project — so no token appears in any committed file. `terraform.tfvars.example` documents the expected shape of the one non-default variable (`grant_principal`) without exposing the real value.

**Validation: full lifecycle exercised, not just a one-way apply.** `terraform destroy` followed immediately by `terraform apply` was run and verified against the live workspace on 2026-07-19 — both completed cleanly (`2 destroyed` / `2 added`), and the `terraform_managed` schema and its grants were independently confirmed in Catalog Explorer after the final apply. This was a deliberate choice over stopping at a single successful `apply`: it demonstrates the configuration is actually reproducible from a clean slate, not just that it worked once.

**What a full production build would additionally provision (documented, not built):** SQL warehouse sizing/autoscaling configuration, network/private-link configuration, workspace-level admin settings, secret scopes, and cluster policies. These are either not exposed on Databricks Free Edition's provisioning model or deliberately out of scope for a two-resource IaC demonstration — see `terraform/README.md` for the full breakdown.

---
