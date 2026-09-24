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

select mm.item_id, mm.content_hash, mm.embedding as movies_metadata_embedding
from {{ source("ml_semantic_embedding", "movies_metadata_tmp") }} as mm
