{% macro create_schema_dev
() %}
    {% if target.name == "local" %}
        CREATE SCHEMA IF NOT EXISTS `{{ target.project }}.tmp_{{ env_var('USER', 'anonymous_user') }}_dev`;
        ALTER SCHEMA `{{ target.project }}.tmp_{{ env_var('USER', 'anonymous_user') }}_dev`
        SET OPTIONS( default_table_expiration_days = 2 );
    {% endif %}
{% endmacro %}
