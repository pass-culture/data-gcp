{% macro create_schema_dev() %}
    {% set raw_user_name = env_var(
        "PERSONAL_DBT_USER", env_var("USER", "anonymous_user")
    ) %}

    {# filter forbidden schema names #}
    {% set user_name = dbt_utils.regex_replace(raw_user_name, "^[^A-Za-z_]+", "") %}
    {% set user_name = user_name | replace("-", "_") %}
    {% set user_name = dbt_utils.regex_replace(user_name, "[^A-Za-z0-9_]", "") %}
    {% if user_name == "" %} {% set user_name = "anonymous_user" %} {% endif %}

    {# Create and alter schema only if target is local #}
    {% if target.name == "local" %}
        CREATE SCHEMA IF NOT EXISTS `{{ target.project }}.tmp_{{ user_name }}_dev`;
        ALTER SCHEMA `{{ target.project }}.tmp_{{ user_name }}_dev`
        SET OPTIONS( default_table_expiration_days = 2 );
    {% endif %}
{% endmacro %}
