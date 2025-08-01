{% macro generate_group_type_columns(params) %}
    {{ params.group_type }} as dimension_name,
    {% if params.group_type == "NAT" %} 'NAT'
    {% else %} {{ params.group_type_name }}
    {% endif %} as dimension_value
{% endmacro %}
