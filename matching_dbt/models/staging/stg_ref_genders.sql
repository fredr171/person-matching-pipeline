-- Raw gender values mapped to M, F or X (config/reference/genders.csv).
select
    value,
    gender
from {{ source('reference', 'genders') }}
