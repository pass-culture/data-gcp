{% macro extract_params_int_value(params) %}
  {% for param in params %}
    MAX(CASE WHEN params.key = '{{ param }}' THEN params.value.int_value END) AS {{ param }}{% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
