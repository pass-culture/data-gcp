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
from {{ ref("int_kpi__labelled_offer_top_ranking") }}
