-- Person ids retired in this load: MERGED (with the surviving id) or ALL_DELETED.
select
    {{ var('load_id') }} as load_id,
    retired_id,
    successor_id,
    reason
from {{ ref('id_reconciliation') }}
where row_type = 'retirement'
