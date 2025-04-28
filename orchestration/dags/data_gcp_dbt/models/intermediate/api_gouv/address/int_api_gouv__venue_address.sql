{{ config(**custom_table_config()) }}

with
    venue_postal_code_geocode as (
        select
            adv.venue_id,
            adv.venue_postal_code,
            safe_cast(pc.postal_approx_centroid_latitude as float64) as venue_latitude,
            safe_cast(
                pc.postal_approx_centroid_longitude as float64
            ) as venue_longitude,
            if(
                pc.postal_approx_centroid_latitude is not null
                and pc.postal_approx_centroid_longitude is not null,
                "postal_code",
                "unknown"
            ) as geocode_type
        from {{ source("raw", "applicative_database_venue") }} as adv
        left join
            {{ ref("int_seed__geo_postal_code") }} as pc
            on adv.venue_postal_code = pc.postal_code
        where adv.venue_is_virtual = false
        qualify row_number() over (partition by adv.venue_id) = 1
    ),

    venue_lat_long_geocode as (
        select
            adv.venue_id,
            adv.venue_street,
            adv.venue_department_code,
            "geolocation" as geocode_type,
            adv.venue_longitude,
            adv.venue_latitude,
            if(
                adv.venue_longitude is not null and adv.venue_latitude is not null,
                adv.venue_postal_code,
                null
            ) as venue_postal_code
        from {{ source("raw", "applicative_database_venue") }} as adv
        where adv.venue_is_virtual = false
    ),

    venue_combined as (
        select
            uag.venue_id,
            uag.venue_department_code,
            uag.venue_street,
            coalesce(uag.geocode_type, upcg.geocode_type) as geocode_type,
            coalesce(
                uag.venue_postal_code, upcg.venue_postal_code
            ) as venue_postal_code,
            coalesce(uag.venue_longitude, upcg.venue_longitude) as venue_longitude,
            coalesce(uag.venue_latitude, upcg.venue_latitude) as venue_latitude
        from venue_lat_long_geocode as uag
        left join venue_postal_code_geocode as upcg on uag.venue_id = upcg.venue_id
    )

select
    venue_id,
    venue_postal_code,
    venue_longitude,
    venue_latitude,
    venue_street,
    geocode_type,
    coalesce(
        case
            when venue_postal_code = "97150"
            then "978"
            when left(venue_postal_code, 2) in ("97", "98")
            then left(venue_postal_code, 3)
            when left(venue_postal_code, 3) in ("200", "201", "209", "205")
            then "2A"
            when left(venue_postal_code, 3) in ("202", "206")
            then "2B"
            else left(venue_postal_code, 2)
        end,
        venue_department_code
    ) as venue_department_code
from venue_combined
