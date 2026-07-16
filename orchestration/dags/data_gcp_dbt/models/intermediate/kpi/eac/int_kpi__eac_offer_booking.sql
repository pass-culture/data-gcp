with
    created_collective_offers as (
        select
            date_trunc(date(collective_offer_creation_date), month) as partition_month,
            venue_region_name,
            venue_academy_name,
            venue_department_code,
            venue_department_name,
            venue_epci_code,
            venue_city_code,
            venue_density_label,
            venue_macro_density_label,
            institution_academy_name,
            institution_department_code,
            institution_department_name,
            institution_epci_code,
            institution_city_code,
            institution_density_label,
            institution_macro_density_label,
            count(collective_offer_id) as total_created_collective_offers,
            0 as total_collective_bookings,
            cast(0 as float64) as total_booking_amount
        from {{ ref("int_global__collective_offer") }}
        group by
            partition_month,
            venue_region_name,
            venue_academy_name,
            venue_department_code,
            venue_department_name,
            venue_epci_code,
            venue_city_code,
            venue_density_label,
            venue_macro_density_label,
            institution_academy_name,
            institution_department_code,
            institution_department_name,
            institution_epci_code,
            institution_city_code,
            institution_density_label,
            institution_macro_density_label
    ),

    booked_collective_offers as (
        select
            date_trunc(
                date(collective_booking_creation_date), month
            ) as partition_month,
            venue_region_name,
            venue_academy_name,
            venue_department_code,
            venue_department_name,
            venue_epci_code,
            venue_city_code,
            venue_density_label,
            venue_macro_density_label,
            institution_academy_name,
            institution_department_code,
            institution_department_name,
            institution_epci_code,
            institution_city_code,
            institution_density_label,
            institution_macro_density_label,
            0 as total_created_collective_offers,
            count(collective_booking_id) as total_collective_bookings,
            sum(booking_amount) as total_booking_amount
        from {{ ref("mrt_global__collective_booking") }}
        where
            collective_booking_status
            in ('CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED')
        group by
            partition_month,
            venue_region_name,
            venue_academy_name,
            venue_department_code,
            venue_department_name,
            venue_epci_code,
            venue_city_code,
            venue_density_label,
            venue_macro_density_label,
            institution_academy_name,
            institution_department_code,
            institution_department_name,
            institution_epci_code,
            institution_city_code,
            institution_density_label,
            institution_macro_density_label
    )

select
    eac_offer_booking.partition_month,
    eac_offer_booking.venue_region_name,
    eac_offer_booking.venue_academy_name,
    eac_offer_booking.venue_department_code,
    eac_offer_booking.venue_department_name,
    eac_offer_booking.venue_epci_code,
    eac_offer_booking.venue_city_code,
    eac_offer_booking.venue_density_label,
    eac_offer_booking.venue_macro_density_label,
    eac_offer_booking.institution_academy_name,
    eac_offer_booking.institution_department_code,
    eac_offer_booking.institution_department_name,
    eac_offer_booking.institution_epci_code,
    eac_offer_booking.institution_city_code,
    eac_offer_booking.institution_density_label,
    eac_offer_booking.institution_macro_density_label,
    sum(
        eac_offer_booking.total_created_collective_offers
    ) as total_created_collective_offers,
    sum(eac_offer_booking.total_collective_bookings) as total_collective_bookings,
    sum(eac_offer_booking.total_booking_amount) as total_booking_amount
from
    (
        select *
        from created_collective_offers
        union all
        select *
        from booked_collective_offers
    ) as eac_offer_booking
group by
    eac_offer_booking.partition_month,
    eac_offer_booking.venue_region_name,
    eac_offer_booking.venue_academy_name,
    eac_offer_booking.venue_department_code,
    eac_offer_booking.venue_department_name,
    eac_offer_booking.venue_epci_code,
    eac_offer_booking.venue_city_code,
    eac_offer_booking.venue_density_label,
    eac_offer_booking.venue_macro_density_label,
    eac_offer_booking.institution_academy_name,
    eac_offer_booking.institution_department_code,
    eac_offer_booking.institution_department_name,
    eac_offer_booking.institution_epci_code,
    eac_offer_booking.institution_city_code,
    eac_offer_booking.institution_density_label,
    eac_offer_booking.institution_macro_density_label
