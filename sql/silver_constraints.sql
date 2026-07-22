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

alter table financial_market_data.dev.silver_equities
    alter column symbol set not null;
alter table financial_market_data.dev.silver_equities
    alter column date set not null;

alter table financial_market_data.dev.silver_treasury_yields
    alter column series_id set not null;
alter table financial_market_data.dev.silver_treasury_yields
    alter column date set not null;
