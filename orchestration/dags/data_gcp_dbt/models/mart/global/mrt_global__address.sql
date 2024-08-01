select
    address_id,
    address_ban_id,
    address_insee_code,
    address_street,
    address_postal_code,
    address_city,
    address_latitude,
    address_longitude,
    address_departement_code
from {{ source('raw', 'applicative_database_address') }}
