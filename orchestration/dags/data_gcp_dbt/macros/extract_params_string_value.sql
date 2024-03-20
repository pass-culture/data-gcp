{% macro extract_params_string_value(params) %}
  {% for param in params %}
    MAX(CASE WHEN params.key = '{{ param }}' THEN params.value.string_value END) AS {{ param }}{% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
