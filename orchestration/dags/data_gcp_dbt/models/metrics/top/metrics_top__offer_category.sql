select
    partition_month,
    geo_level,
    geo_value,
    item_id,
    offer_category_id,
    offer_subcategory_id,
    total_booking_amount,
    total_booking_quantity,
    offer_rank
from {{ ref("int_kpi__offer_category_top_ranking") }}
