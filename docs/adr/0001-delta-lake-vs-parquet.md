# 0001. Delta Lake vs. Parquet as the storage format

**Status:** Accepted
**Date:** 2026-07-17

## Context

This pipeline needed a storage format for Bronze, Silver, and derived
tables (`dim_securities`, `correction_log`) built on Databricks Free
Edition using PySpark. Both source APIs (yfinance, FRED) revise
historical data after initial publication — yfinance's adjusted close
retroactively shifts an entire historical series on a stock split;
FRED republishes revised economic figures. This meant the storage
format needed to support reliable overwrite/update semantics, not just
efficient columnar storage of static data.

Two additional requirements emerged during the build, not anticipated
at the outset: `dim_securities` needed to preserve version history
under a Slowly Changing Dimension Type 2 pattern (append new versions,
update only the `end_date`/`is_current` fields on rows being expired),
and `apply_corrections.py` needed to selectively update individual
historical rows in `silver_equities` when yfinance revised a `volume`
or `high` figure post-load, without touching unrelated rows.

Plain Parquet was the baseline alternative under consideration — a
well-understood columnar format Spark supports natively.

## Decision

Use Delta Lake as the table format for every table in this pipeline
(Bronze, Silver, `dim_securities`, `correction_log`), not plain
Parquet.

## Consequences

**Benefits realized:**

- **ACID transactions** made the Bronze/Silver full-replace strategy
  safe under concurrent or interrupted writes — an `overwrite` that
  fails partway through leaves the table in its prior consistent
  state, not a half-written mix of old and new data. Plain Parquet has
  no equivalent guarantee; a failed overwrite can leave a corrupted or
  partial dataset.
- **`MERGE INTO`** was the mechanism that made both `dim_securities`'s
  SCD2 pattern and `apply_corrections.py`'s targeted row updates
  practical to implement. Plain Parquet has no native update/merge
  operation — achieving the same result would require reading the
  full table, applying the change in memory, and rewriting the entire
  file, which is both more code and a strictly worse safety story
  under failure.
- **Schema evolution controls** (`overwriteSchema=true` used
  throughout this pipeline, deliberately not `mergeSchema`) gave
  explicit, fail-fast control over schema drift — a design choice
  documented in `data_modeling_decisions.md`. Achieving equivalent
  behavior on plain Parquet would require hand-rolled schema
  validation before every write.
- **Time travel** (`VERSION AS OF` / `TIMESTAMP AS OF`) came for free
  as a partial mitigation for the full-replace strategy's lack of a
  persistent audit trail, as discussed in `data_modeling_decisions.md`
  — bounded by default retention windows, but zero additional
  engineering effort to get.

**Costs/risks accepted:**

- **Format lock-in.** Delta's transaction log and file format are not
  directly portable to a non-Delta-aware engine without a compatible
  reader. This is an accepted trade-off, not an oversight — the
  project runs entirely on Databricks, where Delta is the native and
  best-supported format, so portability outside that ecosystem was
  never a real requirement here.
- **Time travel is not a substitute for a true audit trail** — bounded
  retention (30-day log / 7-day `VACUUM` by default), and it only
  captures post-transformation state, not what the raw API actually
  returned. This limitation is inherited from the storage layer and
  is documented in full in `data_modeling_decisions.md`'s Bronze/Silver
  section, since it applies regardless of format choice.
- **Small added conceptual overhead** for a portfolio-scale project —
  Delta's transaction log, `OPTIMIZE`/`VACUUM` semantics, and
  `MERGE INTO` syntax are more to learn than plain Parquet's simpler
  read/write model. Judged worth it here specifically because the
  project's SCD2 and correction-handling requirements genuinely need
  MERGE semantics, not because Delta is unconditionally the right
  choice for every dataset.

## Alternatives Considered

**Plain Parquet.** Simpler mental model, no transaction log overhead,
widely supported outside Databricks. Rejected because it has no native
update/merge/ACID story — every one of this pipeline's actual write
patterns (safe full-replace, SCD2 append-and-expire, targeted
correction updates) either requires or is meaningfully safer with
transactional guarantees Parquet doesn't provide. Choosing Parquet
would have meant re-implementing update semantics by hand (read full
table, mutate in memory, rewrite entirely) for both `dim_securities`
and `apply_corrections.py` — more code, worse failure characteristics,
for a format whose main advantage (simplicity, portability) doesn't
matter much when the entire pipeline runs on Databricks anyway.

**Apache Iceberg / Apache Hudi.** Both are credible open-table-format
alternatives to Delta with similar ACID/MERGE capabilities, and both
are gaining adoption outside the Databricks ecosystem specifically to
avoid Delta's tighter coupling to Databricks. Not seriously evaluated
for this project: Databricks Free Edition's tooling, documentation,
and native integration are built around Delta first, and introducing
a second table format to learn wasn't justified when Delta already
met every functional requirement the project had. Worth naming as the
answer to "what would you use if this weren't running on Databricks."

**Raw CSV/JSON landing (no table format at all).** Considered
implicitly as the "do nothing" baseline. Rejected immediately —
none of Delta's benefits above would exist, and Spark's read
performance and schema enforcement on raw files is materially worse
than on any columnar table format. Not a serious contender at any
point in the design process.
