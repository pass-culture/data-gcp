{% macro custom_table_config() %}
    {% if target.profile_name == 'CI' %}
        {% set config_params = {'materialized': 'view'} %}
    {% else %}
        {% set config_params = {
            'materialized': 'table'
        } %}
    {% endif %}
    {{ return(config_params) }}
{% endmacro %}
