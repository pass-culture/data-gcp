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
                "external_task_id": "movies_metadata.load_movies_metadata",
                "external_dependency_extra_params": {
                    "soft_fail": True,
                    "offset_hours": -6,
                    "poke_interval": 600,
                },
            },
        )
    )
}}
{# item_embedding starts at 4 am (prod and  7am stg) while dbt dag runs at 2:45 am so we use a negative offset of few hours to point the delayed sensor at the correct dagrun (from 6 hours after the dbt dag run  within the previous 24 hours) #}
{% set tmp_source = source("tmp", "movies_metadata_tmp") %}
{# tmp tables expire after 24h: when none is left, merge nothing instead of failing. #}
{% if tmp_tables_exist(tmp_source) %}
    select
        mm.item_id,
        mm.content_hash,
        mm.embedding as movies_metadata_embedding,
        mm.mlflow_run_id,
        mm.embedding_model,
        mm.embedding_date
    from {{ tmp_source }} as mm
    where
        mm._table_suffix = (
            select max(latest._table_suffix) as max_table_suffix
            from {{ tmp_source }} as latest
        )
{% else %}
    select *
    from
        unnest(
            array<
                struct<
                    item_id string,
                    content_hash string,
                    movies_metadata_embedding array<float64>,
                    mlflow_run_id string,
                    embedding_model string,
                    embedding_date date
                >
            >[]
        )
{% endif %}
