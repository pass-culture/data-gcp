{{
    config(
        materialized="table",
        partition_by={
            "field": "partition_month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["booking_type", "municipality_code"],
    )
}}

{% set window_months = var("statistical_secret_window_months") %}

with
    -- complete months only: the current month is still partial
    calendar as (
        select partition_month
        from
            unnest(
                generate_date_array(
                    '2022-01-01',
                    date_sub(date_trunc(current_date(), month), interval 1 month),
                    interval 1 month
                )
            ) as partition_month
    )

select
    calendar.partition_month,
    revenue.booking_type,
    revenue.offerer_id,
    revenue.municipality_code,
    revenue.epci_code,
    revenue.department_code,
    sum(revenue.total_revenue_amount) as revenue_amount
from calendar
inner join
    {{ ref("int_kpi__cultural_partner_offerer_revenue") }} as revenue
    on revenue.partition_month between date_sub(
        calendar.partition_month, interval {{ window_months - 1 }} month
    ) and calendar.partition_month
group by all
having sum(revenue.total_revenue_amount) > 0
