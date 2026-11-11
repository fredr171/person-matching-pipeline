-- Match edges clustering refused because a cannot-link pair would end up in one
-- cluster (SKIPPED_TRANSITIVITY), with the conflicting pair as evidence.
select
    {{ var('load_id') }} as load_id,
    record_key_a,
    record_key_b,
    tier,
    reason_code,
    conflict_record_key_a,
    conflict_record_key_b,
    conflict_reason_code
from {{ ref('clustering_results') }}
where row_type = 'skipped'
