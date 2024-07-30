select
    booking_id,
    stock.offer_id,
    booking_creation_date,
    booking.stock_id,
    booking_quantity,
    user_id,
    booking_amount,
    booking_status,
    booking_is_cancelled,
    booking_is_used,
    booking_used_date,
    booking_cancellation_date,
    booking_cancellation_reason,
    deposit_id,
    offerer_id,
    venue_id,
    price_category_label,
    booking_reimbursement_date,
    coalesce(booking_amount, 0) * coalesce(booking_quantity, 0) as booking_intermediary_amount,
    rank() over (partition by user_id order by booking_creation_date) as booking_rank,
    reimbursed
from {{ source('raw','applicative_database_booking') }} booking
    join {{ source('raw','applicative_database_stock') }} stock using (stock_id)
