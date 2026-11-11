-- Record pairs sharing any blocking key, deduplicated across keys, smaller key first.
with keys as (
    select k.*
    from {{ ref('int_block_keys') }} k
    anti join {{ ref('int_skipped_blocks') }} s using (key_name, key_value)
)

select
    a.record_key as record_key_a,
    b.record_key as record_key_b,
    list(distinct a.key_name order by a.key_name) as blocking_keys
from keys a
join keys b
    on a.key_name = b.key_name
   and a.key_value = b.key_value
   and a.record_key < b.record_key
group by a.record_key, b.record_key
