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

select bm.item_id, bm.content_hash, bm.embedding
from {{ source("ml_semantic_embedding", "books_metadata_tmp") }} as bm
