{{
    config(
        **custom_incremental_config(
            incremental_strategy="merge",
            partition_by=None,
            unique_key="item_id",
            on_schema_change="sync_all_columns",
        )
    )
}}

select
    ie.item_id,
    array(
        select e.element from unnest(ie.semantic_content_sts.list) as e
    ) as semantic_content_sts,
    array(
        select e.element from unnest(ie.semantic_content_clustering.list) as e
    ) as semantic_content_clustering
from {{ source("ml_feat", "item_embedding_tmp") }} as ie
inner join {{ ref("ml_input__item_metadata") }} as im on ie.item_id = im.item_id
