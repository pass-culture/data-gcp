{% macro render_enum_case(
    input_column_name, mapping_list, fallback_sql=none, fallback_str=none
) %}
    {% if fallback_sql is not none and fallback_str is not none %}
        {% do exceptions.raise_compiler_error(
            "render_enum_case error: provide only one fallback, either fallback_sql or fallback_str, not both."
        ) %}
    {% endif %}

    case
        {% for item in mapping_list %}
            {% if not (item.name is string or item.name is iterable) %}
                {% do exceptions.raise_compiler_error(
                    "render_enum_case error: item.name must be a string or an iterable of strings."
                ) %}
            {% elif item.name is string %}
                when {{ input_column_name }} = '{{ item.name }}'
            {% elif item.name is iterable %}
                when
                    {{ input_column_name }} in (
                        {% for val in item.name %}
                            '{{ val }}'{% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
            {% endif %}
            then '{{ item.label }}'
        {% endfor %}
        else
            {% if fallback_sql is not none %} {{ fallback_sql }}
            {% elif fallback_str is not none %} '{{ fallback_str }}'
            {% else %} null
            {% endif %}
    end
{% endmacro %}
