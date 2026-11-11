-- Blocks over the size cap are not compared, and are reported (DESIGN.md section 6).
select key_name, key_value, count(*) as block_size
from {{ ref('int_block_keys') }}
group by key_name, key_value
having count(*) > {{ var('max_block_size') }}
