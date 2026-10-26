{% macro custom_table_config(
    materialized="table",
    partition_by=None,
    cluster_by=None,
    tags=None,
    meta=None
) %}
    {% if target.profile_name == "CI" %}
        {% set config_params = {"materialized": "view"} %}
    {% else %}
        {% set config_params = {
            "materialized": materialized,
        } %}
        {% if cluster_by is not none %}
            {% do config_params.update({"cluster_by": cluster_by}) %}
        {% endif %}
        {% if partition_by is not none %}
            {% do config_params.update({"partition_by": partition_by}) %}
        {% endif %}
    {% endif %}
    {{ return(apply_config_tags_and_meta(config_params, tags=tags, meta=meta)) }}
{% endmacro %}
