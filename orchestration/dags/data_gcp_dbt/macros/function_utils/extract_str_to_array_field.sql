{% macro extract_str_to_array_field(column_name, start, step, end) %}

    {%- set columns = [] %}
    {%- for i in range(start, end, step) %}
        {{- columns.append(column_name ~ "_" ~ (i + 1) ~ "_" ~ (i + step)) or "" }}
    {%- endfor %}
    array(
        select _col
        from
            unnest(
                array_concat(
                    {%- for column in columns %}
                        split(ifnull({{ column }}, ''), ',')
                        {% if not loop.last %}, {% endif %}
                    {%- endfor %}
                )
            ) as _col
        where _col is not null and _col != ''
    )

{% endmacro %}
