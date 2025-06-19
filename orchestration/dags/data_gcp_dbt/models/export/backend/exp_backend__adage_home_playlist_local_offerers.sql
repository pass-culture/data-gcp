-- Get permanent venues with activ template offers --
with
    offerer_offer_info as (
        select
            o.offerer_id,
            o.venue_id,
            v.venue_latitude,
            v.venue_longitude,
            o.collective_offer_id
        from {{ ref("mrt_global__collective_offer") }} o
        join
            {{ ref("int_global__venue") }}
            v on v.venue_id = o.venue_id and v.venue_is_open_to_public is true
        join
            {{ source("raw", "applicative_database_collective_offer_template") }} t
            on t.collective_offer_id = o.collective_offer_id
            and collective_offer_venue_address_type != "school"
        where collective_offer_is_template is true and o.collective_offer_is_active
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

    -- CROSS JOIN
    calculate_distance as (
        select
            i.institution_id,
            i.institution_rural_level,
            o.offerer_id,
            o.venue_id,
            o.collective_offer_id,
            st_distance(
                st_geogpoint(venue_longitude, venue_latitude),
                st_geogpoint(institution_longitude, institution_latitude)
            ) as distance
        from institution_info i
        cross join offerer_offer_info o
    )

-- Filter < 300km
select
    institution_id,
    institution_rural_level,
    offerer_id,
    venue_id,
    collective_offer_id,
    safe_divide(distance, 1000) as distance_in_km
from calculate_distance
where distance < 300000
group by 1, 2, 3, 4, 5, 6
