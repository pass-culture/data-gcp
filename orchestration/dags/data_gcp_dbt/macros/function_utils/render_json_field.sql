{% macro render_json_fields(source_alias, json_field, fields) %}
    {#
  source_alias: table name, reference or CTE alias to prefix JSON field (STRING)
  json_field: JSON column inside CTE
  fields: list of objects
    - json_path: JSON path string
    - alias: output column name
    - null_when_equal: optional, string value to convert to NULL
-#}
    {%- for f in fields -%}
        {%- set expr = (
            "json_value("
            ~ source_alias
            ~ "."
            ~ json_field
            ~ ", '"
            ~ f.json_path
            ~ "') as "
            ~ f.alias
        ) -%}
        {%- if f.null_when_equal is defined -%}
            {%- set expr = (
                "nullif("
                ~ "json_value("
                ~ source_alias
                ~ "."
                ~ json_field
                ~ ", '"
                ~ f.json_path
                ~ "')"
                ~ ", '"
                ~ f.null_when_equal
                ~ "') as "
                ~ f.alias
            ) -%}
        {%- endif -%}
        {{ expr }}
        {%- if not loop.last %},{%- endif %}
    {% endfor -%}
{%- endmacro -%}
