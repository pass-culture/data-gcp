{% macro custom_snapshot_config(
    strategy=None,
    unique_key=None,
    updated_at=None,
    partition_by={"field": "dbt_valid_to", "data_type": "timestamp"},
    cluster_by=None,
    tags=None,
    meta=None,
    check_cols=None,
    hard_deletes="invalidate",
    target_schema=generate_schema_name("raw_applicative_" ~ target.name)
) %}
    {% set config_params = {
        "strategy": strategy,
        "unique_key": unique_key,
        "updated_at": updated_at,
        "partition_by": partition_by,
        "check_cols": check_cols,
        "hard_deletes": hard_deletes,
        "target_schema": target_schema,
    } %}
    {% if cluster_by is not none %}
        {% do config_params.update({"cluster_by": cluster_by}) %}
    {% endif %}
    {{
        return(
            apply_config_tags_and_meta(
                config_params, base_tags=["source_snapshot"], tags=tags, meta=meta
            )
        )
    }}
{% endmacro %}
