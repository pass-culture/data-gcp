/* Intermediate KPI aggregating confirmed collective bookings by educational domain and institution geography. */
with
    booking_aggregated as (
        select
            date_trunc(
                date(cb.collective_booking_creation_date), month
            ) as partition_month,
            cb.scholar_year,
            cod.educational_domain_name,
            cb.institution_region_name,
            coalesce(cast(rd.region_code as string), '-1') as institution_region_code,
            cb.institution_department_name,
            cb.institution_department_code,
            cb.institution_epci as institution_epci_name,
            cb.institution_epci_code,
            cb.institution_city_code,
            coalesce(count(cb.collective_booking_id), 0) as total_collective_bookings,
            coalesce(sum(cb.booking_amount), 0) as total_collective_booking_amount,
            coalesce(
                sum(cb.collective_stock_number_of_tickets), 0
            ) as total_collective_tickets,
            coalesce(
                count(distinct cb.educational_institution_id), 0
            ) as total_collective_institutions
        from {{ ref("mrt_global__collective_booking") }} as cb
        left join
            {{ ref("mrt_global__collective_offer_domain") }} as cod
            on cb.collective_offer_id = cod.collective_offer_id
        left join
            {{ ref("region_department") }} as rd
            on cb.institution_department_code = rd.num_dep
        where
            cb.collective_booking_status
            in ('CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED')
        group by
            date_trunc(date(cb.collective_booking_creation_date), month),
            cb.scholar_year,
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
    scholar_year,
    educational_domain_name,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    institution_epci_name,
    institution_epci_code,
    institution_city_code,
    total_collective_bookings,
    total_collective_booking_amount,
    total_collective_tickets,
    total_collective_institutions,
    sum(total_collective_bookings) over w as cumulative_total_collective_bookings,
    sum(total_collective_booking_amount) over w
    as cumulative_total_collective_booking_amount,
    sum(total_collective_tickets) over w as cumulative_total_collective_tickets,
    sum(total_collective_institutions) over w
    as cumulative_total_collective_institutions
from booking_aggregated
window
    w as (
        partition by
            scholar_year,
            educational_domain_name,
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
