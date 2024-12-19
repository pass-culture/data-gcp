select
    address_id,
    address_ban_id,
    address_insee_code,
    address_street,
    address_postal_code,
    address_city,
    address_latitude,
    address_longitude,
    address_department_code,
    null as undocumented_test_field
from {{ source("raw", "applicative_database_address") }}
