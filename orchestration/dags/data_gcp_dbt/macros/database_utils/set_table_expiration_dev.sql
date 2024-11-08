{% macro set_table_expiration_dev() %}
    {% if target.name == "local" and config.get("materialized") == "view" %}
        ALTER VIEW {{ this }}
        SET OPTIONS(expiration_timestamp = TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP()) + 172800));
    {% elif target.name == "local" %}
        ALTER TABLE {{ this }}
        SET OPTIONS(expiration_timestamp = TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP()) + 172800));
    {% endif %}
{% endmacro %}
