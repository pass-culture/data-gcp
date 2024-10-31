{% test cardinality_equality(model, column_name, to, field) %}

    with
        table as (
            select
                (select count(distinct {{ field }}) from {{ to }}) as num_rows_to,
                (
                    select count(distinct {{ column_name }}) from {{ model }}
                ) as num_rows_table
        ),

        ratio_table as (select num_rows_table / num_rows_to * 100 as ratio from table)
    select 100 - ratio
    from ratio_table
    where 100 - ratio > {{ var("not_null_anomaly_threshold_alert_percentage") }}
{% endtest %}
