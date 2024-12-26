{% macro create_schema_dev() %}
    -- If your username is incomptatible with BigQuery dataset naming constraints:
    -- https://cloud.google.com/bigquery/docs/datasets?hl=en#dataset-naming
    {% set user_name = env_var("PERSONAL_DBT_USER", false) or env_var(
        "USER", "anonymous_user"
    ) %}
    {% if target.name == "local" %}
        CREATE SCHEMA IF NOT EXISTS `{{ target.project }}.tmp_{{ user_name }}_dev`;
        ALTER SCHEMA `{{ target.project }}.tmp_{{ user_name }}_dev`
        SET OPTIONS( default_table_expiration_days = 2 );
    {% endif %}
{% endmacro %}
