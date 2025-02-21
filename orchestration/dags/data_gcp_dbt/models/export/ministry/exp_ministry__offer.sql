select
    offer_id,
    offer_name,
    offer_category_id as offer_category_name,
    offer_created_at,
    is_active
from {{ ref("mrt_global__offer") }}
where is_active
