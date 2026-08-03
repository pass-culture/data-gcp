/* Intermediate KPI aggregating confirmed collective bookings by educational domain and institution geography. */
with
    -- Monthly booking metrics per domain and institution geography, joined to enrich
    -- region codes
    booking_aggregated as (
        select
            date_trunc(
                date(cb.collective_booking_creation_date), month
            ) as partition_month,
            cod.educational_domain_name as domain_name,
            cb.institution_region_name,
            coalesce(cast(rd.region_code as string), '-1') as institution_region_code,  -- '-1' for territories without a matching INSEE region code
            cb.institution_department_name,
            cb.institution_department_code,
            cb.institution_epci as institution_epci_name,
            cb.institution_epci_code,
            cb.institution_city_code,
            count(cb.collective_booking_id) as total_collective_bookings_by_domain,
            sum(cb.booking_amount) as total_collective_booking_amount_by_domain,
            sum(
                cb.collective_stock_number_of_tickets
            ) as total_collective_tickets_by_domain,
            count(
                distinct cb.educational_institution_id
            ) as total_collective_institutions_by_domain  -- distinct institutions with at least one qualifying booking
        from {{ ref("mrt_global__collective_booking") }} as cb
        left join
            {{ ref("mrt_global__collective_offer_domain") }} as cod
            on cb.collective_offer_id = cod.collective_offer_id
        left join
            {{ source("seed", "region_department") }} as rd
            on cb.institution_department_code = rd.num_dep
        where
            cb.collective_booking_status
            in ('CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED')  -- exclude cancelled bookings
        group by
            date_trunc(date(cb.collective_booking_creation_date), month),
            cod.educational_domain_name,
            cb.institution_region_name,
            rd.region_code,
            cb.institution_department_name,
            cb.institution_department_code,
            cb.institution_epci,
            cb.institution_epci_code,
            cb.institution_city_code
    )

select
    partition_month,
    domain_name,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    institution_epci_name,
    institution_epci_code,
    institution_city_code,
    total_collective_bookings_by_domain,
    total_collective_booking_amount_by_domain,
    total_collective_tickets_by_domain,
    total_collective_institutions_by_domain,
    sum(total_collective_bookings_by_domain) over w
    as cumulative_total_collective_bookings_by_domain,
    sum(total_collective_booking_amount_by_domain) over w
    as cumulative_total_collective_booking_amount_by_domain,
    sum(total_collective_tickets_by_domain) over w
    as cumulative_total_collective_tickets_by_domain,
    sum(total_collective_institutions_by_domain) over w
    as cumulative_total_collective_institutions_by_domain
from booking_aggregated
-- Running totals from the earliest available month, partitioned by domain and
-- institution geography
window
    w as (
        partition by
            domain_name,
            institution_region_name,
            institution_region_code,
            institution_department_name,
            institution_department_code,
            institution_epci_name,
            institution_epci_code,
            institution_city_code
        order by partition_month
        rows unbounded preceding
    )
