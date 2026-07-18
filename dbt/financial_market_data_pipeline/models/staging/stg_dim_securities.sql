-- Current-state view of the SCD2 dimension only — historical versions
-- stay queryable directly against dim_securities for point-in-time needs.
select
    security_key,
    symbol,
    company_name,
    sector,
    exchange,
    asset_type,
    effective_date,
    end_date,
    is_current
from {{ source('silver', 'dim_securities') }}
where is_current = true