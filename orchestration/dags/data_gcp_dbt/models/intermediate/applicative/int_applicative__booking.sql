select
    b.booking_id,
    date(b.booking_creation_date) as booking_creation_date,
    b.booking_creation_date as booking_created_at,
    b.stock_id,
    b.booking_quantity,
    b.user_id,
    b.booking_amount,
    b.booking_status,
    b.booking_is_cancelled,
    b.booking_is_used,
    b.reimbursed,
    b.booking_used_date,
    b.booking_cancellation_date,
    b.booking_cancellation_reason,
    b.deposit_id,
    b.offerer_id,
    b.venue_id,
    b.price_category_label,
    b.booking_reimbursement_date,
    coalesce(b.booking_amount, 0)
    * coalesce(b.booking_quantity, 0) as booking_intermediary_amount,
    rank() over (partition by b.user_id order by booking_creation_date) as booking_rank,
    d.deposit_type
from {{ source("raw", "applicative_database_booking") }} as b
left join {{ ref("int_applicative__deposit") }} as d on b.deposit_id = d.deposit_id
