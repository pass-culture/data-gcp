{% if flags.FULL_REFRESH and execute %}
    {{
        exceptions.raise_compiler_error(
            this
            ~ " cannot be run with --full-refresh yet.\n"
            ~ "Embeddings merged before the switch to ml_preproc history are not "
            ~ "in source('ml_preproc', 'item_embedding__movies_metadata'), so a full "
            ~ "refresh would drop them. Backfill that history first (or run the "
            ~ "embedding DAG with embed_all = True), then remove this guard."
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

{#
    Fed by the item_embedding DAG, which appends each run to
    ml_preproc.item_embedding__movies_metadata. No sensor on that DAG: like the other ML
    outputs, new embeddings are merged on the next daily dbt run.
#}
{{
    merge_embedding_select(
        source_relation=source("ml_preproc", "item_embedding__movies_metadata"),
        embedding_column="movies_metadata_embedding",
    )
}}
