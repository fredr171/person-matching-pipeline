-- Nicknames, transliterations and spelling variants from config/reference/nicknames.csv.
select
    canonical,
    canonical_gender,
    variant,
    relation
from {{ source('reference', 'nicknames') }}
