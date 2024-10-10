{% macro custom_snapshot_config(
    strategy=None,
    unique_key=None,
    updated_at=None,
    target_schema=generate_schema_name('raw_' ~ target.name),
    partition_by={'field': 'dbt_valid_to', 'data_type': 'timestamp'},
    tags=['source_snapshot'],
    check_cols=None
) %}
    {% set config_params = {
        "strategy": strategy,
        "unique_key": unique_key,
        "updated_at": updated_at,
        "target_schema": target_schema,
        "partition_by": partition_by,
        "tags": tags,
        "check_cols": check_cols
    } %}
    {{ return(config_params) }}
{% endmacro %}
