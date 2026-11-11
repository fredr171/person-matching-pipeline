-- Generational suffix forms mapped to a canonical suffix (config/reference/suffixes.csv).
select
    form,
    canonical
from {{ source('reference', 'suffixes') }}
