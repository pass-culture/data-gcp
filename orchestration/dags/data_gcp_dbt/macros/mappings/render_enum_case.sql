{% macro render_enum_case(
    input_column_name, mapping_macro, fallback=none, fallback_is_sql=False
) %}
    case
        {% for activity in mapping_macro() %}
            when {{ input_column_name }} = '{{ activity.name }}'
            then '{{ activity.label }}'
        {% endfor %}
        else
            {% if fallback is none %} null
            {% elif fallback_is_sql %} {{ fallback }}
            {% else %} '{{ fallback }}'
            {% endif %}
    end
{% endmacro %}
