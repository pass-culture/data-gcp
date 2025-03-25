select
    venue_id,
    venue_street,
    venue_latitude,
    venue_longitude,
    venue_department_code,
    venue_postal_code,
    venue_city,
    venue_region_name,
    venue_epci,
from {{ ref("int_geo__venue_location") }}
