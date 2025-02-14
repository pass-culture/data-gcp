{% macro obfuscate_id(str_id, secret_str_value) %}
    to_hex(sha256(concat({{ str_id }}, '{{ secret_str_value }}')))
{% endmacro %}
