{% macro extract_str_to_array_field(column_name, start, step, end) %}

    {%- set columns = [] %}
    {%- for i in range(start, end, step) %}
    {{- columns.append(column_name ~ '_' ~ (i + 1) ~ '_' ~ (i + step)) or "" }}
    {%- endfor %}
    ARRAY(
        SELECT _col
        FROM UNNEST(
        ARRAY_CONCAT(
            {%- for column in columns %}
            SPLIT(IFNULL({{ column }}, ''), ','){% if not loop.last %}, {% endif %}
            {%- endfor %}
        )
        ) AS _col
        WHERE _col IS NOT NULL AND _col != ''
    )
    
{% endmacro %}