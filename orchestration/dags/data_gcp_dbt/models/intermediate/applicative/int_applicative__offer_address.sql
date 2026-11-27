select
    offerer_address_id,
    offerer_address_label,
    address_id,
    venue_id,
    offerer_address_type,
    address_street,
    address_postal_code,
    address_city,
    address_department_code,
    address_latitude,
    address_longitude
from {{ ref("int_applicative__all_offerer_address") }}
where offerer_address_type = "OFFER_LOCATION"
