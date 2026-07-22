-- Thin pass-through from silver_equities. Renaming only — no business
-- logic here; that belongs in marts. Corrections are already applied
-- upstream via apply_corrections.py's Delta MERGE, so this is current-state.
select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
from {{ source('silver', 'silver_equities') }}
