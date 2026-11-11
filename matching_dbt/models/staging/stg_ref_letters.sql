-- Accented and special letters mapped to ASCII (config/reference/letters.csv).
select
    "character",
    replacement
from {{ source('reference', 'letters') }}
