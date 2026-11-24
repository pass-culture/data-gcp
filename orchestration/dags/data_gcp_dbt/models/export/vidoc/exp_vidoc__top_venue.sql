select
    partition_month,
    geo_level,
    geo_value,
    venue_id,
    venue_name,
    offerer_name,
    total_booking_amount,
    venue_rank
from {{ ref("metrics_top__venue") }}
