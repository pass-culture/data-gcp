{% macro custom_snapshot_config(
    strategy=None,
    unique_key=None,
    updated_at=None,
    target_schema=generate_schema_name('source_' ~ target.name),
    partition_by={'field': 'dbt_valid_to', 'data_type': 'timestamp'}
) %}
    {% if target.profile_name == 'CI' %}
        {% set config_params = {'materialized': 'view'} %}
    {% else %}
        {% set config_params = {
            "strategy": strategy,
            "unique_key": unique_key,
            "updated_at": updated_at,
            "target_schema": target_schema,
            "partition_by": partition_by
        } %}
    {% endif %}
    {{ return(config_params) }}
{% endmacro %}
