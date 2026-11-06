select
    partition_month,
    geo_level,
    geo_value,
    item_id,
    offer_category_id,
    offer_subcategory_id,
    offer_name,
    venue_tag_name,
    total_booking_amount,
    total_booking_quantity,
    offer_rank
from {{ ref("metrics_top__offer_labelled") }}
