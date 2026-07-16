-- sql/silver_constraints.sql
--
-- One-time setup, not part of the transform.py run path. Run once after
-- the Silver tables first exist (via transform.py), via a notebook cell
-- or any SQL client attached to the warehouse. Safe to re-run: setting
-- NOT NULL on an already-NOT NULL column is a no-op.
--
-- Enforces the key-column contract at the Delta table level so it
-- applies to any future writer, not just transform.py. See
-- docs/data_modeling_decisions.md for the full rationale.

ALTER TABLE financial_market_data.dev.silver_equities ALTER COLUMN symbol SET NOT NULL;
ALTER TABLE financial_market_data.dev.silver_equities ALTER COLUMN date SET NOT NULL;

ALTER TABLE financial_market_data.dev.silver_treasury_yields ALTER COLUMN series_id SET NOT NULL;
ALTER TABLE financial_market_data.dev.silver_treasury_yields ALTER COLUMN date SET NOT NULL;