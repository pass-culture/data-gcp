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

{% set period_months = var("statistical_secret_period_months") %}

with
    -- fixed, non-overlapping periods starting on partition_month: calendar years
    -- for individual bookings, school years (September) for collective bookings.
    -- Only completed periods are kept.
    periods as (
        select period.booking_type, partition_month
        from
            unnest(
                [
                    struct('individual' as booking_type, 1 as start_month),
                    struct('collective', 9)
                ]
            ) as period
        cross join
            unnest(
                generate_date_array(
                    date(2021, period.start_month, 1),
                    date_trunc(current_date(), month),
                    interval {{ period_months }} month
                )
            ) as partition_month
        where
            date_add(partition_month, interval {{ period_months }} month)
            <= date_trunc(current_date(), month)
    )

select
    periods.partition_month,
    revenue.booking_type,
    revenue.offerer_id,
    revenue.municipality_code,
    revenue.epci_code,
    revenue.department_code,
    sum(revenue.total_revenue_amount) as revenue_amount
from periods
inner join
    {{ ref("int_kpi__cultural_partner_offerer_revenue") }} as revenue
    on periods.booking_type = revenue.booking_type
    and revenue.partition_month >= periods.partition_month
    and revenue.partition_month
    < date_add(periods.partition_month, interval {{ period_months }} month)
group by all
having sum(revenue.total_revenue_amount) > 0
