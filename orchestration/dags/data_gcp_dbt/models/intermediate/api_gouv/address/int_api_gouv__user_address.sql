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
    ul.date_updated
from {{ source("raw", "user_locations") }} ul
inner join {{ source("raw", "applicative_database_user")}} adu on ul.user_id = adu.user_id

QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date_updated DESC) = 1
