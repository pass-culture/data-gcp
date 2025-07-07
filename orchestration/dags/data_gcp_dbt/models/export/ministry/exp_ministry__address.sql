select
    address_id,
    address_ban_id as address_ban,
    address_insee_code,
    address_street,
    address_postal_code,
    address_city,
    address_latitude,
    address_longitude,
    address_department_code
from {{ ref("mrt_global__address") }}
