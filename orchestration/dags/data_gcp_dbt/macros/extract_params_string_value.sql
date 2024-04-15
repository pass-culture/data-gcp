{% macro extract_params_string_value(params) %}
    {% for param in params %}
        (SELECT event_params.value.string_value from unnest(event_params) event_params where event_params.key = '{{ param }}') AS {{ param }}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
