select
    offer_id,
    offer_category_id as category,
    offer_subcategory_id as subcategory_id,
    search_group_name
from {{ ref("mrt_global__offer_metadata") }}
