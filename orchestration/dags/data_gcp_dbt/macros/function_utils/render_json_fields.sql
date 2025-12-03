{% macro render_json_fields(source_alias, json_column, fields) %}
    {#
  source_alias: table name, reference or CTE alias to prefix JSON field (string)
  json_column: JSON column inside source_alias (string)
  fields: list of json objects to parse as new columns, each object containing:
    - json_path: JSON path string
    - alias: output column name
    - cast_type: optional, the SQL type to cast the JSON value to (e.g., INT64, FLOAT64, STRING, JSON; default->STRING)
    - null_when_equal: optional, string/or relevant type value that should be converted to NULL using NULLIF
    - final_cast_type: optional, the final SQL type to cast the result to (useful for casting INT64 -> STRING, etc.)
-#}
    {%- for f in fields -%}

        {# Determine whether to use JSON_QUERY (for JSON objects/arrays) or JSON_VALUE (for scalar values) #}
        {%- set operation = "json_value" -%}
        {%- if f.cast_type is defined and f.cast_type | upper == "JSON" -%}
            {%- set operation = "json_query" -%}
        {%- endif -%}

        {# Build the base extraction expression #}
        {%- set base_expr = (
            operation
            ~ "("
            ~ source_alias
            ~ "."
            ~ json_column
            ~ ", '"
            ~ f.json_path
            ~ "')"
        ) -%}

        {# Apply CAST if cast_type is specified and not STRING or JSON #}
        {%- set casted_expr = base_expr -%}
        {%- if f.cast_type is defined and f.cast_type | upper not in (
            "STRING",
            "JSON",
        ) -%}
            {%- set casted_expr = "cast(" ~ base_expr ~ " as " ~ f.cast_type ~ ")" -%}
        {%- endif -%}

        {# Apply NULLIF if null_when_equal is specified #}
        {%- set final_expr = casted_expr -%}
        {%- if f.null_when_equal is defined -%}
            {# Determine if we need to quote the null_when_equal value #}
            {%- if f.null_when_equal is string -%}
                {# String value - add quotes #}
                {%- set null_val = "'" ~ f.null_when_equal ~ "'" -%}
            {%- else -%}
                {# Numeric or other value - no quotes #}
                {%- set null_val = f.null_when_equal -%}
            {%- endif -%}
            {%- set final_expr = "nullif(" ~ casted_expr ~ ", " ~ null_val ~ ")" -%}
        {%- endif -%}

        {# Apply final cast if final_cast_type is specified #}
        {%- if f.final_cast_type is defined -%}
            {%- set final_expr = (
                "cast(" ~ final_expr ~ " as " ~ f.final_cast_type ~ ")"
            ) -%}
        {%- endif -%}

        {{ final_expr }} as {{ f.alias }}
        {%- if not loop.last -%}, {%- endif -%}
    {%- endfor -%}
{% endmacro %}
