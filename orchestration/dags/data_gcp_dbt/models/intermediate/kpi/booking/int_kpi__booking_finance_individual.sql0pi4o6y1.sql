with
    unique_geo_commune as (
        select distinct city_code, city_label, epci_label, region_code
        from {{ ref("int_seed__geo_iris") }}
    )

select
    r.venue_department_code,
    r.venue_department_name,
    r.venue_region_name,
    gi.region_code as venue_region_code,
    r.venue_epci_code,
    gi.epci_label as venue_epci_name,
    r.venue_city_code,
    gi.city_label as venue_city_name,
    r.offerer_is_epn,
    r.offer_category_id,
    date_trunc(r.booking_used_date, month) as partition_month,
    sum(r.total_bookings) as total_bookings,
    sum(r.total_quantities) as total_quantities,
    sum(r.total_revenue_amount) as total_revenue_amount,
    sum(r.total_reimbursed_amount) as total_reimbursed_amount,
    sum(r.total_contribution_amount) as total_contribution_amount
from {{ ref("int_finance__reimbursement") }} as r
left join unique_geo_commune as gi on r.venue_city_code = gi.city_code
group by
    date_trunc(r.booking_used_date, month),
    r.venue_department_code,
    r.venue_department_name,
    r.venue_region_name,
    gi.region_code,
    r.venue_epci_code,
    gi.epci_label,
    r.venue_city_code,
    gi.city_label,
    r.offerer_is_epn,
    r.offer_category_id
