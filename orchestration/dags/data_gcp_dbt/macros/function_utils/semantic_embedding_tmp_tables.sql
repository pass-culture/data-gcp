-- Helpers for the ml_semantic_embedding__<vector> incremental models, which merge
-- the latest per-run staging table written by the item_embedding DAG
-- (tmp_<env>.<vector>_tmp_<ts_nodash>), read through a wildcard source.
--
-- The tmp dataset expires its tables after 24h, so when a vector has not been
-- loaded for a day (failed or skipped embedding run, delayed dbt run), no table
-- matches the wildcard and BigQuery fails the whole query ("does not match any
-- table"). These macros let the model merge nothing instead of failing.
{% macro semantic_embedding_tmp_tables_exist(tmp_source) %}
    {#- Returns whether at least one table matches a wildcard `tmp` source
        (e.g. source("tmp", "books_metadata_tmp") -> books_metadata_tmp_*).
        At parse time nothing is queried: assume tables exist so that the model
        keeps its source() lineage. -#}
    {% if not execute %} {{ return(true) }} {% endif %}

    {% set table_prefix = tmp_source.identifier | replace("*", "") %}
    {% set query %}
        select count(*)
        from `{{ tmp_source.database }}.{{ tmp_source.schema }}.INFORMATION_SCHEMA.TABLES`
        where starts_with(table_name, '{{ table_prefix }}')
    {% endset %}
    {% set n_tables = run_query(query).columns[0].values()[0] %}

    {% if n_tables == 0 %}
        {{
            log(
                "No table matches "
                ~ tmp_source
                ~ ": no new embeddings to merge, the model selects no rows.",
                info=True,
            )
        }}
    {% endif %}
    {{ return(n_tables > 0) }}
{% endmacro %}

{% macro empty_semantic_embedding_rows(embedding_column) %}
    {#- Typed, empty result with the ml_semantic_embedding__<vector> schema:
        merging it inserts, updates and deletes nothing. -#}
    select
        cast(null as string) as item_id,
        cast(null as string) as content_hash,
        array<float64>[] as {{ embedding_column }},
        cast(null as string) as mlflow_run_id,
        cast(null as string) as embedding_model,
        cast(null as date) as embedding_date
    limit 0
{% endmacro %}
