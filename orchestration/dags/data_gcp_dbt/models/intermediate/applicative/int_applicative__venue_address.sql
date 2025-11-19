select
    venue.venue_id,
    oa.address_postal_code as venue_postal_code,
    oa.address_longitude as venue_longitude,
    oa.address_latitude as venue_latitude,
    oa.address_street as venue_street,
    coalesce(
        case
            when oa.address_postal_code = "97150"
            then "978"
            when left(oa.address_postal_code, 2) in ("97", "98")
            then left(oa.address_postal_code, 3)
            when left(oa.address_postal_code, 3) in ("200", "201", "209", "205")
            then "2A"
            when left(oa.address_postal_code, 3) in ("202", "206")
            then "2B"
            else left(oa.address_postal_code, 2)
        end,
        oa.address_department_code
    ) as venue_department_code
from {{ source("raw", "applicative_database_venue") }} as venue
left join {{ ref("int_applicative__offerer_address") }} as oa
    on venue.offerer_address_id = oa.offerer_address_id
