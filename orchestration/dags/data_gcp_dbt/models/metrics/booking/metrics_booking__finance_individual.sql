with
    venue_revenue_by_city as (
        select
            r.venue_id,
            r.venue_city_code,
            date_trunc(r.booking_used_date, month) as partition_month,
            sum(r.booking_intermediary_amount) as total_revenue_amount_by_venue
        from {{ ref("int_global__booking") }} as r
        where r.booking_is_used is true and r.booking_used_date >= "2021-01-01"
        group by date_trunc(r.booking_used_date, month), r.venue_id, r.venue_city_code
    ),

    territory_revenue as (
        select
            partition_month,
            venue_city_code,
            sum(total_revenue_amount_by_venue) as total_revenue_amount_by_territory,
            max(total_revenue_amount_by_venue) as max_revenue_amount_by_territory
        from venue_revenue_by_city
        group by partition_month, venue_city_code
    ),

    booking_finance_with_revenue_share as (
        select
            bf.partition_month,
            bf.venue_department_code,
            bf.venue_department_name,
            bf.venue_region_name,
            bf.venue_epci_code,
            bf.venue_city_code,
            bf.offerer_is_epn,
            bf.offer_category_id,
            bf.total_bookings,
            bf.total_quantities,
            bf.total_revenue_amount,
            bf.total_reimbursed_amount,
            bf.total_contribution_amount,
            tr.max_total_revenue_amount_by_territory,
            tr.total_revenue_amount_by_territory
        from {{ ref("int_kpi__booking_finance_individual") }} as bf
        left join
            territory_revenue as tr
            on bf.partition_month = tr.partition_month
            and bf.venue_city_code = tr.venue_city_code
    )

select
    bf.partition_month,
    bf.venue_department_code,
    bf.venue_department_name,
    bf.venue_region_name,
    bf.venue_epci_code,
    bf.venue_city_code,
    bf.offerer_is_epn,
    bf.offer_category_id,
    bf.total_bookings,
    bf.total_quantities,
    bf.total_revenue_amount,
    bf.total_reimbursed_amount,
    bf.total_contribution_amount,
    coalesce(cp.total_active_partners_global <= 3, false)
    or coalesce(
        safe_divide(
            bf.max_total_revenue_amount_by_territory,
            nullif(bf.total_revenue_amount_by_territory, 0)
        )
        > 0.85,
        false
    ) as is_statistic_secret
from booking_finance_with_revenue_share as bf
left join
    {{ ref("int_kpi__cultural_partner_activation") }} as cp
    on bf.venue_city_code = cp.partner_city_code
    and bf.partition_month = cp.partition_month
