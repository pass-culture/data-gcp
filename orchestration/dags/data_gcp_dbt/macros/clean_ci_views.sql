
{% macro delete_views(models) %}
    {% if target.profile_name == 'CI' %}
        {% for model in models %}
            {% set model_ref = ref(model) %}
            DROP VIEW IF EXISTS {{ model_ref }} CASCADE;
        {% endfor %}
    {% endif %}
{% endmacro %}
