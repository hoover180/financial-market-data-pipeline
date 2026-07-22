-- Renaming value -> yield_value: the source column name is too generic to
-- carry downstream into marts, where it would sit ambiguously alongside
-- other numeric columns.
select
    series_id,
    date,
    value as yield_value
from {{ source('silver', 'silver_treasury_yields') }}
