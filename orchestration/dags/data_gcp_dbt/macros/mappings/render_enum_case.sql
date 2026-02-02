{% macro render_enum_case(
    input_column_name, mapping_list, fallback_sql=none, fallback_str=none
) %}
    {% if fallback_sql is not none and fallback_str is not none %}
        {% do exceptions.raise_compiler_error(
            "render_enum_case error: provide only one fallback, either fallback_sql or fallback_str, not both."
        ) %}
    {% endif %}

    case
        {% for activity in mapping_list %}
            when {{ input_column_name }} = '{{ activity.name }}'
            then '{{ activity.label }}'
        {% endfor %}
        else
            {% if fallback_sql is not none %} {{ fallback_sql }}
            {% elif fallback_str is not none %} '{{ fallback_str }}'
            {% else %} null
            {% endif %}
    end
{% endmacro %}
