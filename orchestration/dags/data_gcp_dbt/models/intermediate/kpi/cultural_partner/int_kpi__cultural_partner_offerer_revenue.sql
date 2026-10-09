with
    used_bookings as (
        select
            'individual' as booking_type,
            booking.offerer_id,
            booking.venue_municipality_code as municipality_code,
            booking.venue_epci_code as epci_code,
            booking.venue_department_code as department_code,
            date_trunc(date(booking.booking_used_date), month) as partition_month,
            booking.booking_intermediary_amount as revenue_amount
        from {{ ref("int_global__booking") }} as booking
        where booking.booking_is_used is true

        union all

        select
            'collective' as booking_type,
            collective_booking.offerer_id,
            collective_booking.venue_municipality_code as municipality_code,
            collective_booking.venue_epci_code as epci_code,
            collective_booking.venue_department_code as department_code,
            date_trunc(
                date(collective_booking.collective_booking_used_date), month
            ) as partition_month,
            collective_booking.booking_amount as revenue_amount
        from {{ ref("mrt_global__collective_booking") }} as collective_booking
        where collective_booking.is_used_collective_booking is true
    ),

    -- one EPCI and one department per municipality, so geographic levels nest
    geo_municipality as (
        select
            municipality_code,
            any_value(epci_code) as epci_code,
            any_value(department_code) as department_code
        from {{ ref("int_seed__geo_iris") }}
        where municipality_code is not null
        group by municipality_code
    ),

    -- fallback for municipalities missing from the geographic referential
    booking_municipality as (
        select
            municipality_code,
            any_value(epci_code) as epci_code,
            any_value(department_code) as department_code
        from used_bookings
        where municipality_code is not null
        group by municipality_code
    )

select
    used_bookings.partition_month,
    used_bookings.booking_type,
    used_bookings.offerer_id,
    used_bookings.municipality_code,
    -- ZZZZZZZZZ is the INSEE code for municipalities outside any EPCI
    nullif(coalesce(geo.epci_code, booking_geo.epci_code), 'ZZZZZZZZZ') as epci_code,
    coalesce(geo.department_code, booking_geo.department_code) as department_code,
    sum(used_bookings.revenue_amount) as total_revenue_amount
from used_bookings
inner join
    booking_municipality as booking_geo
    on used_bookings.municipality_code = booking_geo.municipality_code
left join
    geo_municipality as geo on used_bookings.municipality_code = geo.municipality_code
where used_bookings.partition_month is not null
group by all
