-- Joins daily equity returns against same-day Treasury yield changes on
-- the 10-year series (DGS10). Grain: one row per symbol per date.
with returns as (
    select * from {{ ref('fct_daily_returns') }}
),

yields as (
    select
        date,
        yield_value,
        yield_value - lag(yield_value) over (order by date) as yield_change
    from {{ ref('stg_treasury_yields') }}
    where series_id = 'DGS10'
)

select
    r.symbol,
    r.date,
    r.daily_return,
    y.yield_value as treasury_10y_yield,
    y.yield_change as treasury_10y_yield_change
from returns as r
left join yields as y
    on r.date = y.date
