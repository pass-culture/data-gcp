select
    booking_id,
    booking_creation_date,
    booking_quantity,
    booking_amount,
    booking_status,
    booking_cancellation_date,
    booking_cancellation_reason,
    user_id,
    deposit_id,
    booking_intermediary_amount,
    booking_rank,
    booking_used_date,
    stock_id,
    offer_id,
    venue_id,
    offerer_id
from {{ ref("mrt_global__booking") }}
