-- Values that mean "unknown" for names, SSNs and DOBs (config/reference/placeholders.csv).
select
    field,
    value,
    reason
from {{ source('reference', 'placeholders') }}
