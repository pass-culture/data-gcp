{{ config(**custom_table_config()) }}

select
    adu.user_id,
    adu.user_address,
    adu.user_postal_code,
    adu.user_department_code,
    ul.longitude as user_longitude,
    ul.latitude as user_latitude,
    ul.city_code,
    ul.api_adresse_city,
    ul.date_updated as updated_date
from {{ source("raw", "user_locations") }} as ul
inner join
    {{ source("raw", "applicative_database_user") }} as adu on ul.user_id = adu.user_id

qualify row_number() over (partition by ul.user_id order by ul.date_updated desc) = 1
