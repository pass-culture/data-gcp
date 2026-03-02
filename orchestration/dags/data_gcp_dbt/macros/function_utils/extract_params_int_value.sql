{% macro extract_params_int_value(params, alias=true, fallback_keys={}) %}
    {% for param in params %}
        {% if param in fallback_keys %}
            coalesce(
                (
                    select event_params.value.int_value
                    from unnest(event_params) event_params
                    where event_params.key = '{{ param }}'
                ),
                (
                    select event_params.value.int_value
                    from unnest(event_params) event_params
                    where event_params.key = '{{ fallback_keys[param] }}'
                )
            )
        {% else %}
            (
                select event_params.value.int_value
                from unnest(event_params) event_params
                where event_params.key = '{{ param }}'
            )
        {% endif %}
        {% if alias %} as {{ param }} {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
