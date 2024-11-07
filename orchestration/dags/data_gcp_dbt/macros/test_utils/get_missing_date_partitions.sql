{% macro get_missing_date_partitions(model, date_column_name, where_condition=null) %}
    {% set has_missing_partitions = check_partition_count(
        model,
        partition_column_name=date_column_name,
        where_condition=where_condition,
    ) %}

    {% if has_missing_partitions and execute %}

        {% set query %}
            with date_span as (
                select
                    min({{ date_column_name }}) as min_date,
                    max({{ date_column_name }}) as max_date
                from {{ model }}
                {% if where_condition %}
                where {{ where_condition }}
                {% endif %}
            ),
            all_dates as (
                select
                    date as date
                from
                    date_span,
                    unnest(generate_date_array(date_span.min_date, date_span.max_date, INTERVAL 1 day)) as date
            ),
            existing_partitions as (
                select distinct {{ date_column_name }} as date
                from {{ model }}
                {% if where_condition %}
                where {{ where_condition }}
                {% endif %}
            ),
            unmatched_partitions as (
                select
                    all_dates.date as missing_partition_date,
                    existing_partitions.date as existing_partition_date
                from
                    all_dates
                full outer join
                    existing_partitions
                on all_dates.date = existing_partitions.date
                where
                    existing_partitions.date is null or all_dates.date is null
            )
            select missing_partition_date, existing_partition_date from unmatched_partitions
        {% endset %}

        {% set result = run_query(query) %}
        {% set output = [] %}

        {% if result is not none and result.rows | length > 0 %}
            {% set output = result.rows %}
        {% endif %}

        {{ return(output) }}
    {% else %} {{ return([]) }}
    {% endif %}
{% endmacro %}
