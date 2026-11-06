-- The variable part of a per-vector embedding merge: the latest-sharded tmp
-- export (max _table_suffix) when one exists, else an empty typed row set
-- (tmp tables expire after 24h) so the merge has something to run against.
{% macro merge_embedding_select(tmp_source, embedding_column) %}
    {% if tmp_tables_exist(tmp_source) %}
        select
            src.item_id,
            src.content_hash,
            src.embedding as {{ embedding_column }},
            src.mlflow_run_id,
            src.embedding_model,
            src.embedding_date
        from {{ tmp_source }} as src
        where
            src._table_suffix = (
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
                        {{ embedding_column }} array<float64>,
                        mlflow_run_id string,
                        embedding_model string,
                        embedding_date date
                    >
                >[]
            )
    {% endif %}
{% endmacro %}
