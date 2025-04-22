{{ config(**custom_table_config()) }}

with
    user_postal_code_geocode as (
        select
            adu.user_id,
            adu.user_department_code,
            pc.postal_code as user_postal_code,
            pc.postal_approx_centroid_latitude as user_latitude,
            pc.postal_approx_centroid_longitude as user_longitude,
            timestamp(adu.user_creation_date) as user_creation_at,
            if(
                pc.postal_approx_centroid_latitude is not null
                and pc.postal_approx_centroid_longitude is not null,
                "postal_code",
                "unknown"
            ) as geocode_type
        from {{ source("raw", "applicative_database_user") }} as adu
        left join
            {{ ref("int_seed__geo_postal_code") }} as pc
            on adu.user_postal_code = pc.postal_code
        qualify
            row_number() over (
                partition by adu.user_id order by adu.user_creation_date desc
            )
            = 1
    ),

    user_address_geocode as (
        select
            ul.user_id,
            if(
                ul.result_postcode != "" and ul.result_postcode is not null,
                ul.result_postcode,
                null
            ) as user_postal_code,
            if(
                ul.result_citycode != "" and ul.result_citycode is not null,
                ul.result_citycode,
                null
            ) as user_city_code,
            if(
                ul.result_city != "" and ul.result_city is not null,
                ul.result_city,
                null
            ) as user_city,
            if(
                ul.user_full_address != "" and ul.user_full_address is not null,
                ul.user_full_address,
                null
            ) as user_address_raw,
            if(
                ul.result_type != "" and ul.result_type is not null,
                ul.result_type,
                null
            ) as geocode_type,
            if(
                ul.longitude != "" and ul.longitude is not null,
                safe_cast(ul.longitude as float64),
                null
            ) as user_longitude,
            if(
                ul.latitude != "" and ul.latitude is not null,
                safe_cast(ul.latitude as float64),
                null
            ) as user_latitude,
            timestamp(ul.updated_at) as user_address_geocode_updated_at
        from {{ source("raw", "user_address") }} as ul
        where ul.result_status = "ok"
        qualify
            row_number() over (partition by ul.user_id order by ul.updated_at desc) = 1
    ),

    address_combined as (
        select
            upcg.user_id,
            upcg.user_creation_at,
            upcg.user_department_code,
            uag.user_address_geocode_updated_at,
            uag.user_address_raw,
            coalesce(uag.geocode_type, upcg.geocode_type) as geocode_type,
            coalesce(uag.user_postal_code, upcg.user_postal_code) as user_postal_code,
            coalesce(uag.user_longitude, upcg.user_longitude) as user_longitude,
            coalesce(uag.user_latitude, upcg.user_latitude) as user_latitude
        from user_postal_code_geocode as upcg
        left join user_address_geocode as uag on upcg.user_id = uag.user_id
    )

select
    user_id,
    user_creation_at,
    geocode_type,
    user_postal_code,
    user_longitude,
    user_latitude,
    user_address_geocode_updated_at,
    user_address_raw,
    coalesce(
        case
            when user_postal_code = "97150"
            then "978"
            when left(user_postal_code, 2) in ("97", "98")
            then left(user_postal_code, 3)
            when left(user_postal_code, 3) in ("200", "201", "209", "205")
            then "2A"
            when left(user_postal_code, 3) in ("202", "206")
            then "2B"
            else left(user_postal_code, 2)
        end,
        user_department_code
    ) as user_department_code
from address_combined
