select
    offer_id,
    offer_name
    as offer_category_id,
    offer_created_at,
    is_active
from {{ ref("mrt_global__offer") }}
where is_active
