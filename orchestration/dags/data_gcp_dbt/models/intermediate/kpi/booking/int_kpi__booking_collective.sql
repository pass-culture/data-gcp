with
    bookings as (
        select
            cb.scholar_year,
            cb.venue_region_name,
            cb.venue_department_name,
            cb.venue_department_code,
            cb.venue_epci as venue_epci_name,
            cb.venue_epci_code,
            cb.venue_city as venue_city_name,
            cb.venue_city_code,
            date_trunc(
                date(cb.collective_booking_creation_date), month
            ) as partition_month,
            coalesce(rd.region_code, -1) as venue_region_code,
            count(cb.collective_booking_id) as total_collective_bookings,
            sum(cb.booking_amount) as total_collective_amount_spent
        from {{ ref("mrt_global__collective_booking") }} as cb
        left join
            {{ source("seed", "region_department") }} as rd
            on cb.venue_department_code = rd.num_dep
        where
            cb.collective_booking_status
            in ('CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED')
        group by
            date_trunc(date(cb.collective_booking_creation_date), month),
            cb.scholar_year,
            cb.venue_region_name,
            rd.region_code,
            cb.venue_department_name,
            cb.venue_department_code,
            cb.venue_epci,
            cb.venue_epci_code,
            cb.venue_city,
            cb.venue_city_code
    )

select
    bookings.partition_month,
    bookings.scholar_year,
    bookings.venue_region_name,
    bookings.venue_region_code,
    bookings.venue_department_name,
    bookings.venue_department_code,
    bookings.venue_epci_name,
    bookings.venue_epci_code,
    bookings.venue_city_name,
    bookings.venue_city_code,
    bookings.total_collective_bookings,
    bookings.total_collective_amount_spent
from bookings
