{% macro parse_timestamp_dms(column) %}
    case
        when {{ column }} is null
        then null
        when length(cast({{ column }} as string)) = 19
        then timestamp_micros(cast({{ column }} / 1000 as int64))
        when length(cast({{ column }} as string)) = 16
        then timestamp_micros({{ column }})
        when length(cast({{ column }} as string)) = 13
        then timestamp_millis({{ column }})
        when length(cast({{ column }} as string)) = 10
        then timestamp_seconds({{ column }})
        else null
    end
{% endmacro %}
