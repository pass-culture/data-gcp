{% macro record_key(user_id_column) %}
    mod(abs(farm_fingerprint(cast({{ user_id_column }} as string))), 256)
{% endmacro %}
