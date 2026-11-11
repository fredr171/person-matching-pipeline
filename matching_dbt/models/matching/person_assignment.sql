-- Source record -> stable person id for this load (DESIGN.md section 11).
select
    {{ var('load_id') }} as load_id,
    r.source_system,
    r.source_record_id,
    r.record_life,
    r.record_key,
    a.record_uid,
    a.person_id,
    c.cluster_id
from {{ ref('id_reconciliation') }} a
join {{ ref('int_records_standardized') }} r using (record_uid)
join {{ ref('clusters') }} c using (record_key)
where a.row_type = 'assignment'
