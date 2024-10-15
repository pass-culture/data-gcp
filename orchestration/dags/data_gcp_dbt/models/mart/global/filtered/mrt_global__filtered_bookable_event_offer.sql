select
    offer_id,
    offer_name,
    offer_creation_date,
    venue_id,
    venue_name,
    passculture_pro_url,
    webapp_url,
    venue_department_code,
    venue_region_name,
    venue_type_label,
    offer_description,
    last_stock_price,
    offer_category_id
from {{ ref("mrt_global__offer") }}
where event and offer_is_bookable
