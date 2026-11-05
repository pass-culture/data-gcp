with
    bookings as (
        select
            b.booking_id,
            b.booking_is_used,
            b.booking_intermediary_amount,
            b.offer_category_id,
            b.venue_region_name,
            b.venue_department_name,
            b.venue_epci_code,
            b.venue_municipality_code,
            u.user_region_name as region_name,
            u.user_department_name as department_name,
            u.user_epci_code as epci_code,
            u.user_municipality_code as municipality_code,
            -- users missing from mrt_global__user_beneficiary (deleted,
            -- suspended, GRANT_FREE) keep their bookings, without geography
            u.user_id is not null as user_is_beneficiary,
            b.booking_amount = 0 as booking_is_free,
            date_trunc(date(b.booking_used_date), month) as used_month,
            date_trunc(date(b.booking_creation_date), month) as creation_month
        from {{ ref("mrt_global__booking") }} as b
        left join
            {{ ref("mrt_global__user_beneficiary") }} as u on b.user_id = u.user_id
    ),

    -- each booking is counted on its own date: used bookings on the month of
    -- use, all bookings (cancelled included) on the month of creation
    flows as (
        select
            used_month as partition_month,
            user_is_beneficiary,
            region_name,
            department_name,
            epci_code,
            municipality_code,
            venue_region_name,
            venue_department_name,
            venue_epci_code,
            venue_municipality_code,
            offer_category_id,
            booking_id as used_booking_id,
            booking_intermediary_amount as used_amount,
            null as created_booking_id,
            false as created_booking_is_free
        from bookings
        where booking_is_used

        union all

        select
            creation_month as partition_month,
            user_is_beneficiary,
            region_name,
            department_name,
            epci_code,
            municipality_code,
            venue_region_name,
            venue_department_name,
            venue_epci_code,
            venue_municipality_code,
            offer_category_id,
            null as used_booking_id,
            null as used_amount,
            booking_id as created_booking_id,
            booking_is_free as created_booking_is_free
        from bookings
    )

select
    partition_month,
    user_is_beneficiary,
    region_name,
    department_name,
    epci_code,
    municipality_code,
    venue_region_name,
    venue_department_name,
    venue_epci_code,
    venue_municipality_code,
    offer_category_id,
    count(used_booking_id) as total_used_bookings,
    sum(used_amount) as total_used_amount,
    count(created_booking_id) as total_created_bookings,
    countif(created_booking_is_free) as total_created_free_bookings
from flows
group by
    partition_month,
    user_is_beneficiary,
    region_name,
    department_name,
    epci_code,
    municipality_code,
    venue_region_name,
    venue_department_name,
    venue_epci_code,
    venue_municipality_code,
    offer_category_id
