SELECT
    booking_id,
    DATE(booking_creation_date) AS booking_creation_date,
    booking_creation_date AS booking_created_at,
    stock_id,
    booking_quantity,
    user_id,
    booking_amount,
    booking_status,
    booking_is_cancelled,
    booking_is_used,
    reimbursed,
    booking_used_date,
    booking_cancellation_date,
    booking_cancellation_reason,
    deposit_id,
    offerer_id,
    venue_id,
    price_category_label,
    booking_reimbursement_date,
    coalesce(booking_amount, 0) * coalesce(booking_quantity, 0) AS booking_intermediary_amount,
    rank() OVER (PARTITION BY user_id ORDER BY booking_creation_date) AS booking_rank,
    d.type AS deposit_type
FROM {{ source('raw','applicative_database_booking') }} AS b
-- todo : mettre un LEFT ici + le mettre dans l'ancien modèle
INNER JOIN {{ ref('int_applicative__deposit') }} AS d ON b.deposit_id = d.id
