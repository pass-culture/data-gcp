{% macro record_key(user_id_column) %}
    {#- Salted so that cell keys cannot be recomputed from the public code -#}
    mod(
        abs(
            farm_fingerprint(
                concat(
                    "{{ env_var('DBT_RECORD_KEY_SALT', '') }}",
                    cast({{ user_id_column }} as string)
                )
            )
        ),
        256
    )
{% endmacro %}
