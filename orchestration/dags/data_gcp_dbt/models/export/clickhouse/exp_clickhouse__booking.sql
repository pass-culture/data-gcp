SELECT
    DATE("{{ ds }}") as update_date,
    offerer_id,
    offer_id,
    date(booking_creation_date) as creation_date,
    date(booking_used_date) as used_date,
    booking_status,
    deposit_type,
    booking_quantity,
    booking_amount
FROM {{ ref('mrt_global__boking') }}
WHERE not cast(booking_is_cancelled as bool) and booking_status != 'CANCELLED'
