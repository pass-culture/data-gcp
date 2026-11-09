-- Select a vector's last embedding run, or an empty typed row set until the
-- item_embedding DAG has created its table.
{% macro merge_embedding_select(source_relation, embedding_column) %}
    {% if not execute or adapter.get_relation(
        database=source_relation.database,
        schema=source_relation.schema,
        identifier=source_relation.identifier,
    ) is not none %}
        select
            item_id,
            content_hash,
            embedding as {{ embedding_column }},
            mlflow_run_id,
            embedding_model,
            embedding_date
        from {{ source_relation }}
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
