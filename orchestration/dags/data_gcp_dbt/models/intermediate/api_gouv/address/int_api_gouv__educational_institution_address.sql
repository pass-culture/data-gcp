{{ config(**custom_table_config()) }}

with
    institution_postal_code_geocode as (
        select
            adei.educational_institution_id,
            pc.postal_code as institution_postal_code,
            pc.postal_approx_centroid_latitude as institution_latitude,
            pc.postal_approx_centroid_longitude as institution_longitude,
            if(
                pc.postal_approx_centroid_latitude is not null
                and pc.postal_approx_centroid_longitude is not null,
                "municipality",
                "unknown"
            ) as geocode_type
        from {{ source("raw", "applicative_database_educational_institution") }} as adei
        left join
            {{ ref("int_seed__geo_postal_code") }} as pc
            on adei.institution_postal_code = pc.postal_code
        qualify row_number() over (partition by adei.educational_institution_id) = 1
    ),

    institution_lat_long_geocode as (
        select
            adei.educational_institution_id,
            adei.institution_department_code,
            "geolocation" as geocode_type,
            adei.institution_longitude,
            adei.institution_latitude,
            if(
                adei.institution_longitude is not null
                and adei.institution_latitude is not null,
                adei.institution_postal_code,
                null
            ) as institution_postal_code
        from {{ source("raw", "applicative_database_educational_institution") }} as adei
    ),

    institution_combined as (
        select
            uag.educational_institution_id,
            uag.institution_department_code,
            uag.geocode_type,
            coalesce(
                uag.institution_postal_code, upcg.institution_postal_code
            ) as institution_postal_code,
            coalesce(
                uag.institution_longitude, upcg.institution_longitude
            ) as institution_longitude,
            coalesce(
                uag.institution_latitude, upcg.institution_latitude
            ) as institution_latitude
        from institution_lat_long_geocode as uag
        left join
            institution_postal_code_geocode as upcg
            on uag.educational_institution_id = upcg.educational_institution_id
    )

select
    educational_institution_id,
    institution_postal_code,
    institution_longitude,
    institution_latitude,
    geocode_type,
    coalesce(
        case
            when institution_postal_code = "97150"
            then "978"
            when left(institution_postal_code, 2) in ("97", "98")
            then left(institution_postal_code, 3)
            when left(institution_postal_code, 3) in ("200", "201", "209", "205")
            then "2A"
            when left(institution_postal_code, 3) in ("202", "206")
            then "2B"
            else left(institution_postal_code, 2)
        end,
        institution_department_code
    ) as institution_department_code
from institution_combined
