select
    oa.offerer_address_id,
    oa.offerer_address_label,
    oa.address_id,
    oa.offerer_id,
    oa.venue_id,
    a.address_street,
    a.address_postal_code,
    a.address_city,
    a.address_department_code,
    a.address_latitude,
    a.address_longitude
from {{ source("raw", "applicative_database_offerer_address") }} as oa
left join
    {{ source("raw", "applicative_database_address") }} as a
    on oa.address_id = a.address_id
