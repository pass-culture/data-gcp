{{ config(**custom_table_config()) }}


select
    ul.user_id,
    ul.result_postcode as user_postal_code,
    CAST(ul.longitude as float) as user_longitude,
    CAST(ul.latitude as float) as user_latitude,
    ul.result_citycode as user_city_code,
    ul.result_city as user_city,
    ul.user_full_address as user_raw_address,
    ul.result_type as user_geo_position_quality,
    coalesce(
        case
            when ul.result_postcode = '97150'
            then '978'
            when left(ul.result_postcode, 2) in ('97', '98')
            then left(ul.result_postcode, 3)
            when left(ul.result_postcode, 3) in ('200', '201', '209', '205')
            then '2A'
            when left(ul.result_postcode, 3) in ('202', '206')
            then '2B'
            else left(ul.result_postcode, 2)
        end,
        adu.user_department_code
    ) as user_department_code,
    timestamp(ul.updated_at) as updated_at,
    date(ul.updated_at) as updated_date
from {{ source("raw", "user_address") }} as ul
inner join
    {{ source("raw", "applicative_database_user") }} as adu on ul.user_id = adu.user_id
where ul.result_status = 'ok'
qualify row_number() over (partition by ul.user_id order by ul.updated_at desc) = 1
