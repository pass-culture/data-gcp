{% macro check_partition_count(model, partition_column_name, where_condition=null) %}
    {% if execute %}
        {% set query %}
            with date_span as (
                select
                    min({{ partition_column_name }}) as min_date,
                    max({{ partition_column_name }}) as max_date,
                    count(distinct {{ partition_column_name }}) as partition_cnt
                from {{ model }}
                {% if where_condition %}
                where {{ where_condition }}
                {% endif %}
            ),
            missing_count as (
                select
                    partition_cnt - CAST(DATE_DIFF(max_date, min_date,day) + 1 AS INT64) as missing_partition_count
                from date_span
            )
            select missing_partition_count from missing_count
        {% endset %}

        {% set result = run_query(query) %}

        {% if result is not none and result.rows | length > 0 %}
            {% set missing_partition_count = result.rows[0][
                "missing_partition_count"
            ] %}
            {% if missing_partition_count > 0 %} {{ return(true) }}
            {% else %} {{ return(false) }}
            {% endif %}
        {% else %} {{ return(false) }}
        {% endif %}
    {% else %} {{ return(false) }}
    {% endif %}
{% endmacro %}
