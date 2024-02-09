select 
    cluster.*,
    topic.* except(semantic_cluster_id,semantic_category)
from {{source('clean','item_clusters')}} as cluster
left join {{ source('clean','item_topics_labels') }} as topic on cluster.semantic_cluster_id = topic.semantic_cluster_id

