-- Get one random template offer per venue, that can take place at school
with
    random_template_offer_per_venue as (
        select collective_offer_id, collective_offer_creation_date, venue_id
        from {{ source("raw", "applicative_database_collective_offer_template") }} as o
        where o.collective_offer_location_type = 'school' and collective_offer_is_active
        qualify row_number() over (partition by venue_id order by rand()) = 1
    ),

    -- Get venues which moved to educational institutions, last 12 months, and the
    -- random active template offer
    offerer_venue_info as (
        select
            b.offerer_id,
            b.venue_id,
            v.collective_offer_id,
            b.educational_institution_id as institution_id,
            id.institution_latitude as venue_moving_latitude,
            id.institution_longitude as venue_moving_longitude,
            max(collective_booking_creation_date) as last_booking_date,
            count(distinct b.collective_booking_id) as nb_booking
        from {{ ref("mrt_global__collective_booking") }} as b
        inner join
            {{ source("raw", "applicative_database_collective_offer") }} as o
            on b.collective_offer_id = o.collective_offer_id
            and o.collective_offer_location_type = 'school'
        inner join random_template_offer_per_venue as v on b.venue_id = v.venue_id  -- JOIN because we only keep venues that have bookings AND template offer
        left join
            {{ ref("mrt_global__educational_institution") }} as id
            on b.educational_institution_id = id.institution_id
        where
            collective_booking_status in ('CONFIRMED', 'REIMBURSED', 'USED')
            and collective_booking_creation_date
            >= date_sub(current_date(), interval 12 month)
        group by 1, 2, 3, 4, 5, 6
    ),

    -- Get institutions
    institution_info as (
        select
            institution_id,
            institution_density_label as institution_rural_level,
            institution_latitude,
            institution_longitude
        from {{ ref("mrt_global__educational_institution") }}
    ),

    -- Get all venues with at least one reservation at less than 300KM.
    ac_moving as (
        select
            i.institution_id,
            i.institution_rural_level,
            o.venue_id,
            o.collective_offer_id,
            o.last_booking_date,
            o.nb_booking,
            o.venue_moving_latitude,
            o.venue_moving_longitude
        from offerer_venue_info as o
        inner join institution_info as i on o.institution_id = i.institution_id
        where
            st_distance(
                st_geogpoint(o.venue_moving_longitude, o.venue_moving_latitude),
                st_geogpoint(i.institution_longitude, i.institution_latitude)
            )
            < 300000
            and nb_booking >= 1
    ),

    -- For all institutions, get all offerers
    all_institutions as (
        select
            i.institution_id,
            i.institution_rural_level,
            m.venue_id,
            m.collective_offer_id,
            m.nb_booking as reserved_nb_booking,
            m.last_booking_date,
            m.institution_id as reserved_institution_id,
            st_distance(
                st_geogpoint(m.venue_moving_longitude, m.venue_moving_latitude),
                st_geogpoint(i.institution_longitude, i.institution_latitude)
            ) as distance
        from institution_info as i
        cross join ac_moving as m  -- not the same institution

        where i.institution_id != m.institution_id
    )

-- Filter < 300KM
select
    institution_id,
    institution_rural_level,
    venue_id,
    collective_offer_id,
    safe_divide(distance, 1000) as distance_in_km,
    last_booking_date
from all_institutions
where distance < 300000
group by 1, 2, 3, 4, 5, 6
