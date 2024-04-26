{% macro get_mediation_url() %}
    {% if target.name == 'prod' %}
        {{ return("'passculture-metier-prod-production'") }}
    {% elif target.name == 'staging' %}
        {{ return("'passculture-metier-ehp-staging'") }}
    {% else %}
        {{ return("'passculture-metier-ehp-testing'") }}
    {% endif %}
{% endmacro %}
