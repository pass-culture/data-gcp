with
    booking_aggregated as (
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
            coalesce(cast(rd.region_code as string), '-1') as venue_region_code,
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
    ),

    booking_with_cumul as (
        select
            booking_aggregated.partition_month,
            booking_aggregated.scholar_year,
            booking_aggregated.venue_region_name,
            booking_aggregated.venue_region_code,
            booking_aggregated.venue_department_name,
            booking_aggregated.venue_department_code,
            booking_aggregated.venue_epci_name,
            booking_aggregated.venue_epci_code,
            booking_aggregated.venue_city_name,
            booking_aggregated.venue_city_code,
            booking_aggregated.total_collective_bookings,
            booking_aggregated.total_collective_amount_spent,
            sum(booking_aggregated.total_collective_bookings) over (
                partition by
                    booking_aggregated.scholar_year,
                    booking_aggregated.venue_region_name,
                    booking_aggregated.venue_region_code,
                    booking_aggregated.venue_department_name,
                    booking_aggregated.venue_department_code,
                    booking_aggregated.venue_epci_name,
                    booking_aggregated.venue_epci_code,
                    booking_aggregated.venue_city_name,
                    booking_aggregated.venue_city_code
                order by booking_aggregated.partition_month
                rows unbounded preceding
            ) as cumulative_total_collective_bookings,
            sum(booking_aggregated.total_collective_amount_spent) over (
                partition by
                    booking_aggregated.scholar_year,
                    booking_aggregated.venue_region_name,
                    booking_aggregated.venue_region_code,
                    booking_aggregated.venue_department_name,
                    booking_aggregated.venue_department_code,
                    booking_aggregated.venue_epci_name,
                    booking_aggregated.venue_epci_code,
                    booking_aggregated.venue_city_name,
                    booking_aggregated.venue_city_code
                order by booking_aggregated.partition_month
                rows unbounded preceding
            ) as cumulative_total_collective_amount_spent
        from booking_aggregated
    )

select
    booking_with_cumul.partition_month,
    booking_with_cumul.scholar_year,
    booking_with_cumul.venue_region_name,
    booking_with_cumul.venue_region_code,
    booking_with_cumul.venue_department_name,
    booking_with_cumul.venue_department_code,
    booking_with_cumul.venue_epci_name,
    booking_with_cumul.venue_epci_code,
    booking_with_cumul.venue_city_name,
    booking_with_cumul.venue_city_code,
    booking_with_cumul.total_collective_bookings,
    booking_with_cumul.total_collective_amount_spent,
    booking_with_cumul.cumulative_total_collective_bookings,
    booking_with_cumul.cumulative_total_collective_amount_spent
from booking_with_cumul
