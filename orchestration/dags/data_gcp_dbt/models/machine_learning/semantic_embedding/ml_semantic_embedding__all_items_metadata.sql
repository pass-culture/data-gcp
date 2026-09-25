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
    ie.item_id,
    ie.content_hash,
    ie.embedding as all_items_metadata_embedding,
    ie.mlflow_run_id,
    ie.embedding_model,
    ie.embedding_date
from {{ source("ml_semantic_embedding", "all_items_metadata_tmp") }} as ie
