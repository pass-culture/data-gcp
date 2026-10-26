-- Summing a scholar year's monthly rows must give the scholar year totals of the
-- mart, on both geography sides. Returns the scholar years that do not match.
with
    mart as (
        select
            cb.scholar_year,
            count(*) as total_prereservations,
            countif(
                cb.collective_booking_status
                in ('CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED')
            ) as total_collective_bookings
        from {{ ref("mrt_global__collective_booking") }} as cb
        inner join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on cb.scholar_year = ey.scholar_year
        where cb.collective_booking_status != 'CANCELLED'
        group by cb.scholar_year
    ),

    kpis as (
        select
            'venue' as geo_side,
            scholar_year,
            sum(total_prereservations) as total_prereservations,
            sum(total_collective_bookings) as total_collective_bookings,
            sum(total_actions) as total_actions
        from {{ ref("int_kpi__booking_collective") }}
        group by scholar_year
        union all
        select
            'institution' as geo_side,
            scholar_year,
            sum(total_prereservations) as total_prereservations,
            sum(total_collective_bookings) as total_collective_bookings,
            sum(total_actions) as total_actions
        from {{ ref("int_kpi__booking_collective_institution") }}
        group by scholar_year
    )

select
    geo_side,
    mart.scholar_year,
    mart.total_prereservations as mart_total_prereservations,
    kpis.total_prereservations as kpi_total_prereservations,
    mart.total_collective_bookings as mart_total_collective_bookings,
    kpis.total_collective_bookings as kpi_total_collective_bookings,
    kpis.total_actions as kpi_total_actions
from mart
cross join unnest(['venue', 'institution']) as geo_side
left join kpis on mart.scholar_year = kpis.scholar_year and geo_side = kpis.geo_side
where
    kpis.scholar_year is null
    or kpis.total_prereservations != mart.total_prereservations
    or kpis.total_collective_bookings != mart.total_collective_bookings
    or kpis.total_actions != mart.total_collective_bookings
