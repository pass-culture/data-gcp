{% test kpi_variation_threshold(model, column_name, threshold=0.3) %}

    with
        monthly_values as (
            select
                partition_month,
                dimension_name,
                dimension_value,
                kpi_name,
                {{ column_name }} as current_val,
                lag({{ column_name }}) over (
                    partition by dimension_name, dimension_value, kpi_name
                    order by partition_month
                ) as prev_val
            from {{ model }}
        )

    select *
    from monthly_values
    where
        prev_val is not null
        and prev_val != 0
        -- Calcul de la variation absolue > 30%
        and abs((current_val - prev_val) / prev_val) > {{ threshold }}

{% endtest %}
