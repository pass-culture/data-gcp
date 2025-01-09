{% macro create_schema_dev
() %}
    {% if target.name == "local" %}
        {% set user_name = env_var("PERSONAL_DBT_USER", "unset_user") %}
        CREATE SCHEMA IF NOT EXISTS `{{ target.project }}.tmp_{{ user_name }}_dev`;
        ALTER SCHEMA `{{ target.project }}.tmp_{{ user_name }}_dev`
        SET OPTIONS( default_table_expiration_days = 2 );
    {% endif %}
{% endmacro %}
