{% macro clean_str(text) %}
    case when {{ text }} in ('nan', 'None', '', 'none') then null else {{ text }} end
{% endmacro %}
