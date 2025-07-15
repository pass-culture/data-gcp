{% macro extract_params_int_value(params,alias=true) %}
    {% for param in params %}
        (
            select event_params.value.int_value
            from unnest(event_params) event_params
            where event_params.key = '{{ param }}'
        ){% if alias %} as {{ param }} {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
