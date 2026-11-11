-- Clustering must never put a blocked pair into one cluster (DESIGN.md section 10).
select p.record_key_a, p.record_key_b
from {{ ref('match_pairs') }} p
join {{ ref('clusters') }} a on a.record_key = p.record_key_a
join {{ ref('clusters') }} b on b.record_key = p.record_key_b
where p.cannot_link and a.cluster_id = b.cluster_id
