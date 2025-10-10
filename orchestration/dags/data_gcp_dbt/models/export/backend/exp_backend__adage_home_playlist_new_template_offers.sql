-- last collective_offer_template by offerer_id created past 2 months
with
    offerer_offer_info as (
        select
            o.offerer_id,
            o.venue_id,
            v.venue_latitude,
            v.venue_longitude,
            o.collective_offer_id,
            o.collective_offer_creation_date
        from {{ ref("mrt_global__collective_offer") }} as o
        left join {{ ref("int_global__venue") }} as v on o.venue_id = v.venue_id
        where
            o.collective_offer_creation_date
            >= date_sub(current_date(), interval 2 month)  -- uniquement sur les 2 derniers mois
            and collective_offer_is_template is true
        qualify
            row_number() over (
                partition by offerer_id order by collective_offer_creation_date desc
            )
            = 1
    ),  -- on garde seulement la plus récente

    -- Add location of the venue_representation (venue_id of the field venue_adress)
    -- when it is different from the initial venue of the offer
    add_representation_venue as (
        select
            o.*,
            offerer_address_id,
            oa.address_latitude as offerer_address_latitude,
            oa.address_longitude as offerer_address_longitude
        from offerer_offer_info as o
        left join
            {{ source("raw", "applicative_database_collective_offer_template") }} as a
            on o.collective_offer_id = a.collective_offer_id
            and a.offerer_address_id is not null
        left join
            {{ ref("int_applicative__offerer_address") }} as oa
            on a.offerer_address_id = oa.offerer_address_id
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
            o.offerer_address_id,
            o.collective_offer_id,
            o.collective_offer_creation_date,
            st_distance(
                st_geogpoint(venue_longitude, venue_latitude),
                st_geogpoint(institution_longitude, institution_latitude)
            ) as distance,
            st_distance(
                st_geogpoint(offerer_address_longitude, offerer_address_latitude),
                st_geogpoint(institution_longitude, institution_latitude)
            ) as distance_v2
        from institution_info as i
        cross join add_representation_venue as o
    )

-- Filter < 300km
select
    institution_id,
    institution_rural_level,
    offerer_id,
    venue_id,
    collective_offer_id,
    collective_offer_creation_date,
    safe_divide(distance, 1000) as distance_in_km,
    safe_divide(distance_v2, 1000) as distance_v2_in_km
from calculate_distance
where distance < 300000
group by 1, 2, 3, 4, 5, 6, 7, 8
