select
    partition_month,
    geo_level,
    geo_value,
    venue_id,
    venue_name,
    offerer_name,
    venue_tag_name,
    total_booking_amount,
    venue_rank
from {{ ref("int_kpi__labelled_venue_top_ranking") }}
