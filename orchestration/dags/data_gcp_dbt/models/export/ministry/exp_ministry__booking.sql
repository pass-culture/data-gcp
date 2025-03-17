{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"}
    )
}}

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
    deposit_type,
    booking_intermediary_amount,
    booking_rank,
    booking_used_date,
    stock_beginning_date,
    stock_id,
    offer_id,
    venue_id,
    offerer_id,
    partner_id,
    offer_subcategory_id,
    offer_category_id,
    same_category_booking_rank,
    user_booking_rank,
    isbn
from {{ ref("mrt_global__booking") }}
