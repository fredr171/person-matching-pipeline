-- Record key -> cluster id (the cluster's smallest record key), with cluster size.
select
    record_key,
    cluster_id,
    count(*) over (partition by cluster_id) as cluster_size
from {{ ref('clustering_results') }}
where row_type = 'cluster'
