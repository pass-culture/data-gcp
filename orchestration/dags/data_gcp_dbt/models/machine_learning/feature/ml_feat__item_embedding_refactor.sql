{{
    config(
        **custom_incremental_config(
            incremental_strategy="merge",
            partition_by=None,
            unique_key="item_id",
            on_schema_change="append_new_columns",
        )
    )
}}

select ie.item_id, ie.content_hash, ie.semantic_content
from {{ source("ml_feat", "item_embedding_tmp") }} as ie
inner join {{ ref("ml_input__item_metadata") }} as im on ie.item_id = im.item_id
