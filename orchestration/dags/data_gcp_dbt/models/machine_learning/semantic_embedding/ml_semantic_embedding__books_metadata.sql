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
            tags=["external_dependency"],
            meta={
                "external_dag_id": "item_embedding",
                "external_task_id": "books_metadata.load_books_metadata",
                "external_dependency_on_skip": "skip",
                "external_dependency_offset_hours": -6,
            },
        )
    )
}}
{# item_embedding starts at 4 am (prod and  7am stg) while dbt dag runs at 2:45 am so we use a negative offset of few hours to point the delayed sensor at the correct dagrun (from 6 hours after the dbt dag run  within the previous 24 hours) #}
select
    bm.item_id,
    bm.content_hash,
    bm.embedding as books_metadata_embedding,
    bm.mlflow_run_id,
    bm.embedding_model,
    bm.embedding_date
from {{ source("tmp", "books_metadata_tmp") }} as bm
where
    bm._table_suffix = (
        select max(latest._table_suffix) as max_table_suffix
        from {{ source("tmp", "books_metadata_tmp") }} as latest
    )
