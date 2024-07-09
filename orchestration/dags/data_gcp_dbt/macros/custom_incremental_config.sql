{% macro custom_incremental_config(
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    on_schema_change='ignore',
    cluster_by=None
) %}
    {% if target.profile_name == 'CI' %}
        {% set materialization = 'view' %}
    {% else %}
        {% set materialization = 'incremental' %}
    {% endif %}

    {% set config_params = {
        'materialized': materialization,
    } %}

    {% if materialization == 'incremental' %}
        {% do config_params.update({
            'incremental_strategy': incremental_strategy,
            'partition_by': partition_by,
            'on_schema_change': on_schema_change
        }) %}
    {% endif %}

    {% if cluster_by is not none %}
        {% do config_params.update({'cluster_by': cluster_by}) %}
    {% endif %}

    {{ return(config_params) }}
{% endmacro %}
