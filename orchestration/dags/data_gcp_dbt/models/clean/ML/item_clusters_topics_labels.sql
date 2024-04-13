select 
    cluster.*,
    topic_labels.* except(semantic_cluster_id,semantic_category),
from {{source('clean','item_clusters')}} as cluster
left join {{ source('clean','item_topics') }} as topic 
on cluster.item_id = topic.item_id 
left join {{ source('clean','item_topics_labels') }} as topic_labels
on topic.topic_id = topic_labels.topic_id

