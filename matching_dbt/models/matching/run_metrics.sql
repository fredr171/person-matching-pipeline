-- Counts for this run (DESIGN.md section 13): one row per (metric_group, metric).
with pairs as (select * from {{ ref('match_pairs') }}),
assignment as (select * from {{ ref('person_assignment') }}),
previous as (select * from {{ ref('stg_previous_assignment') }}),

metrics as (
select 'records' as metric_group, 'live_records' as metric, count(*)::bigint as value
from {{ ref('int_records_standardized') }}
union all
select 'pairs', 'compared', count(*) from pairs
union all
select 'pairs', 'matched', count(*) filter (where is_match) from pairs
union all
select 'pairs', 'cannot_link', count(*) filter (where cannot_link) from pairs
union all
select 'reason_code', reason_code, count(*) from pairs group by reason_code
union all
select 'skipped_blocks', key_name, count(*) from {{ ref('int_skipped_blocks') }} group by key_name
union all
select 'skipped_blocks', 'records_in_skipped_blocks', coalesce(sum(block_size), 0)
from {{ ref('int_skipped_blocks') }}
union all
select 'clustering', 'SKIPPED_TRANSITIVITY', count(*) from {{ ref('skipped_joins') }}
union all
select 'clustering', 'clusters', count(distinct cluster_id) from {{ ref('clusters') }}
union all
select 'cluster_size', cast(cluster_size as varchar), count(distinct cluster_id)
from {{ ref('clusters') }} group by cluster_size
union all
select 'person_ids', 'new', count(distinct person_id)
from assignment
where person_id not in (select person_id from previous)
union all
select 'person_ids', 'retired_' || lower(reason), count(*)
from {{ ref('person_id_crosswalk') }} group by reason
union all
select 'person_ids', 'records_with_changed_id', count(*)
from assignment a join previous p using (record_uid)
where a.person_id <> p.person_id
)

select metric_group, metric, cast(value as bigint) as value
from metrics
order by metric_group, metric
