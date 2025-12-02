{% macro custom_incremental_config(
    incremental_strategy="insert_overwrite",
    partition_by={"field": "event_date", "data_type": "date"},
    on_schema_change="ignore",
    cluster_by=None,
    unique_key=None,
    require_partition_filter=false
) %}
    {% if target.profile_name == "CI" %}
        {% set config_params = {"materialized": "view"} %}
    {% else %}
        {% set config_params = {
            "materialized": "incremental",
            "incremental_strategy": incremental_strategy,
            "on_schema_change": on_schema_change,
            "require_partition_filter": require_partition_filter,
        } %}
        {% if partition_by is not none %}
            {% do config_params.update({"partition_by": partition_by}) %}
        {% endif %}
        {% if cluster_by is not none %}
            {% do config_params.update({"cluster_by": cluster_by}) %}
        {% endif %}
        {% if unique_key is not none %}
            {% do config_params.update({"unique_key": unique_key}) %}
        {% endif %}
    {% endif %}
    {{ return(config_params) }}
{% endmacro %}
