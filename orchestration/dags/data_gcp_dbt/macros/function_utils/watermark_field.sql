{% macro watermark_field(str_field, fields_watermark_config=None) %}
    {% if fields_watermark_config is none or str_field not in fields_watermark_config.keys() %}
        {{ str_field }}
    {% else %} {{ fields_watermark_config[str_field] | replace("{}", str_field) }}
    {% endif %}
{% endmacro %}
