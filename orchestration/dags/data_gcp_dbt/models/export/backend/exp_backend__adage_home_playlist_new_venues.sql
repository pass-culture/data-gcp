with
    offerer_offer_info as (
        select
            o.offerer_id,
            o.venue_id,
            v.venue_latitude,
            v.venue_longitude,
            v.venue_creation_date,
            o.collective_offer_id,
            o.collective_offer_creation_date
        from {{ ref("mrt_global__collective_offer") }} o
        left join {{ ref("int_global__venue") }} v on v.venue_id = o.venue_id
        where
            collective_offer_is_template is true
            and v.venue_is_open_to_public
            and v.venue_creation_date >= date_sub(current_date(), interval 4 month)
        qualify
            row_number() over (
                partition by venue_id order by collective_offer_creation_date asc
            )
            = 1
    ),

    -- Add location of the venue_representation (venue_id of the field venue_adress)
    -- when it is different from the initial venue of the offer
    add_representation_venue as (
        select
            o.*,
            collective_offer_venue_humanized_id as venue_v2_id,
            v.venue_latitude as venue_v2_latitude,
            v.venue_longitude as venue_v2_longitude
        from offerer_offer_info o
        left join
            {{ source("raw", "applicative_database_collective_offer_template") }} a
            on o.collective_offer_id = a.collective_offer_id
            and collective_offer_venue_humanized_id is not null
            and collective_offer_venue_humanized_id != a.venue_id
        left join
            {{ ref("int_global__venue") }} v
            on v.venue_id = a.collective_offer_venue_humanized_id
    ),

    -- Get institutions
    institution_info as (
        select
            institution_id,
            institution_density_label as institution_rural_level,
            institution_latitude,
            institution_longitude
        from {{ ref("mrt_global__educational_institution") }} id
    ),

    -- CROSS JOIN
    calculate_distance as (
        select
            i.institution_id,
            i.institution_rural_level,
            o.offerer_id,
            o.venue_id,
            o.venue_creation_date,
            o.venue_v2_id,
            o.collective_offer_id,
            o.collective_offer_creation_date,
            st_distance(
                st_geogpoint(venue_longitude, venue_latitude),
                st_geogpoint(institution_longitude, institution_latitude)
            ) as distance,
            st_distance(
                st_geogpoint(venue_v2_longitude, venue_v2_latitude),
                st_geogpoint(institution_longitude, institution_latitude)
            ) as distance_v2
        from institution_info i
        cross join add_representation_venue o
    )

-- Filter < 300km
select
    institution_id,
    institution_rural_level,
    offerer_id,
    venue_id,
    venue_creation_date,
    collective_offer_id,
    collective_offer_creation_date,
    safe_divide(distance, 1000) as distance_in_km,
    safe_divide(distance_v2, 1000) as distance_v2_in_km
from calculate_distance
where distance < 300000
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
