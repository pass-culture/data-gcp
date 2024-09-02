select
    offer_id,
    offer_name,
    offer_creation_date,
    venue_name,
    passculture_pro_url,
    webapp_url,
    venue_department_code,
    venue_region_name,
    offer_description
from {{ ref('mrt_global__offer') }}
where event
    AND offer_is_bookable
