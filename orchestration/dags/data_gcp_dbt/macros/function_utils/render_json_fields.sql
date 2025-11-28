{% macro render_json_fields(source_alias, json_column, fields) %}
    {#
  source_alias: table name, reference or CTE alias to prefix JSON field (string)
  json_column: JSON column inside source_alias (string)
  fields: list of json objects to parse as new columns, each object containing:
    - json_path: JSON path string
    - alias: output column name
    - cast_type: optional, the SQL type to cast the JSON value to (e.g., int64, numeric, default->string)
    - null_when_equal: optional, string/or relevant type value that should be converted to NULL using NULLIF
-#}
    {%- for f in fields -%}
        {%- set base_expr = "json_value(" ~ source_alias ~ "." ~ json_column ~ ", '" ~ f.json_path ~ "')" -%}
        {%- set null_val = None -%}
        {%- if f.cast_type is defined -%}
            {%- set cast_expr = "cast(" ~ base_expr ~ " as " ~ f.cast_type ~ ")" -%}
            {%- if f.null_when_equal is defined -%}
                {%- set null_val = "cast('" ~ f.null_when_equal ~ "' as " ~ f.cast_type ~ ")" -%}
            {%- endif -%}
        {%- else -%}
            {%- set cast_expr = base_expr -%}
            {%- if f.null_when_equal is defined -%}
                {%- set null_val = "'" ~ f.null_when_equal ~ "'" -%}
            {%- endif -%}
        {%- endif -%}
        {%- if null_val is not none -%}
            {%- set expr = "nullif(" ~ cast_expr ~ ", " ~ null_val ~ ")" -%}
        {%- else -%}
            {%- set expr = cast_expr -%}
        {%- endif -%}
        {{ expr ~ " as " ~ f.alias }}
        {%- if not loop.last %},{%- endif %}
    {% endfor -%}
{%- endmacro -%}
