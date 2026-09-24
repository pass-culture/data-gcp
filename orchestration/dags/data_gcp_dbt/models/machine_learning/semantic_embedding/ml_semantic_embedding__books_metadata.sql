{% if flags.FULL_REFRESH and execute %}
    {{
        exceptions.raise_compiler_error(
            this
            ~ " cannot be run with --full-refresh.\n"
            ~ "It would remove existing embeddings from the table.\n"
            ~ "To refresh all embeddings, you must first run the embedding DAG "
            ~ "for all items (embed_all = True), then delete this table and "
            ~ "rerun it without the --full-refresh flag."
        )
    }}
{% endif %}

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

select
    bm.item_id,
    bm.content_hash,
    bm.embedding as books_metadata_embedding,
    bm.mlflow_run_id,
    bm.embedding_model
from {{ source("ml_semantic_embedding", "books_metadata_tmp") }} as bm
