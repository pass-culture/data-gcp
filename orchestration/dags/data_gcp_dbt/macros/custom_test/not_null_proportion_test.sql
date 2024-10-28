{% test not_null_proportion(model, column_name, where_condition, warn_if) %}
    {% set threshold_test = var("threshold_test") %}

    with
        validation as (
            select
                count(*) as total_records,
                sum(
                    case when {{ column_name }} is null then 1 else 0 end
                ) as matching_records
            from {{ model }}
            where {{ where_condition }}
        ),
        final as (
            select
                cast(matching_records as float64)
                / nullif(total_records, 0) as proportion
            from validation
        )
    select *
    from final
    where proportion > {{ threshold_test }}

{% endtest %}
