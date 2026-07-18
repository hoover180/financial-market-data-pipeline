-- Grain: one row per symbol per trading day.
-- Daily return = (close - prior close) / prior close, using LAG over
-- symbol-partitioned date order. Requires >= 2 trading days of history
-- per symbol; first day per symbol will have a null return by design.
with equities as (
    select * from {{ ref('stg_equities') }}
),

with_prior_close as (
    select
        symbol,
        date,
        close,
        lag(close) over (partition by symbol order by date) as prior_close
    from equities
)

select
    symbol,
    date,
    close,
    prior_close,
    (close - prior_close) / prior_close as daily_return
from with_prior_close