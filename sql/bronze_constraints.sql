-- sql/bronze_constraints.sql
--
-- One-time setup, not part of the load.py run path. Run once after
-- the Bronze tables first exist (via load.py), via a notebook cell
-- or any SQL client attached to the warehouse. Safe to re-run.
--
-- Needed because BRONZE_SCHEMAS in load.py declares symbol/date and
-- series_id/date as non-nullable, but that declaration only enforces
-- types on write -- it does not tighten nullability on an existing
-- Delta table. Delta's overwriteSchema=true allows adding columns or
-- widening types, not narrowing an existing column's nullability;
-- that requires this explicit ALTER TABLE, same mechanism used for
-- Silver (sql/silver_constraints.sql).
--
-- See docs/data_modeling_decisions.md for the full rationale.

alter table financial_market_data.dev.bronze_equities
    alter column symbol set not null;
alter table financial_market_data.dev.bronze_equities
    alter column date set not null;

alter table financial_market_data.dev.bronze_treasury_yields
    alter column series_id set not null;
alter table financial_market_data.dev.bronze_treasury_yields
    alter column date set not null;
