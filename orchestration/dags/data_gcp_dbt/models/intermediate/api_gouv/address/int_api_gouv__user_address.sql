{{ config(**custom_table_config()) }}


with user_postal_code_geocode as (
    select
        adu.user_id,
        adu.user_creation_at,
        adu.user_department_code,
        safe_cast(pc.centroid_latitude as float64) as user_latitude,
        safe_cast(pc.centroid_longitude as float64) as user_longitude,
        pc.postal_code as user_postal_code,
        if(pc.centroid_latitude is not null and pc.centroid_longitude is not null, "municipality", "unknown") as geocode_type
    from {{ source("raw", "applicative_database_user") }} as adu
    left join {{ source("seed", "2025_insee_postal_code") }} as pc on adu.user_postal_code = pc.postal_code
    qualify row_number() over (partition by adu.user_id order by adu.user_creation_at desc) = 1
),

user_address_geocode as (
    select
        ul.user_id,
        ul.result_postcode as user_postal_code,
        safe_cast(ul.longitude as float64) as user_longitude,
        safe_cast(ul.latitude as float64) as user_latitude,
        ul.result_citycode as user_city_code,
        ul.result_city as user_city,
        ul.user_full_address as user_raw_address,
        ul.result_type as geocode_type
    from {{ source("raw", "user_address") }} as ul
    where ul.result_status = 'ok'
    qualify row_number() over (partition by ul.user_id order by ul.updated_at desc) = 1
),


address_combined as (
select
    upcg.user_id,
    upcg.user_creation_at,
    upcg.user_department_code,
    coalesce(uag.geocode_type, upcg.geocode_type) as geocode_type,
    coalesce(uag.user_postal_code, upcg.user_postal_code) as user_postal_code,
    coalesce(uag.user_longitude, upcg.user_longitude) as user_longitude,
    coalesce(uag.user_latitude, upcg.user_latitude) as user_latitude,
    timestamp(uag.updated_at) as user_address_geocode_updated_at,
from user_postal_code_geocode as upcg
left join user_address_geocode as uag on upcg.user_id = uag.user_id

)


select
    user_id,
    user_creation_at,
    geocode_type,
    user_postal_code,
    coalesce(
        case
            when user_postal_code = '97150'
            then '978'
            when left(user_postal_code, 2) in ('97', '98')
            then left(  , 3)
            when left(result_postcode, 3) in ('200', '201', '209', '205')
            then '2A'
            when left(result_postcode, 3) in ('202', '206')
            then '2B'
            else left(result_postcode, 2)
        end,
        user_department_code
    ) as user_department_code,
    user_longitude,
    user_latitude,
    user_address_geocode_updated_at
from address_combined
