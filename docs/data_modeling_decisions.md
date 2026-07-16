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
