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

select ie.item_id, ie.content_hash, ie.embedding
from {{ source("ml_semantic_embedding", "all_items_metadata_tmp") }} as ie
