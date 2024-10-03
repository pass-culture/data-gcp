select
    oa.offerer_address_id,
    oa.offerer_address_label,
    oa.address_id,
    oa.offerer_id,
    a.address_street,
    a.address_postal_code,
    a.address_city,
    a.address_department_code
from {{ source("raw", "applicative_database_offerer_address") }} oa
left join
    {{ source("raw", "applicative_database_address") }} a
    on oa.address_id = a.address_id
