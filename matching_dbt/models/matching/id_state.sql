-- The first unused person id sequence number; read by the next load.
select next_sequence
from {{ ref('id_reconciliation') }}
where row_type = 'state'
