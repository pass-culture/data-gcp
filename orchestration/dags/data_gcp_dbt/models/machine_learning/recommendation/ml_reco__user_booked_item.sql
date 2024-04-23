{{
    config(
        materialized = "view"
    )
}}

SELECT
    DISTINCT b.user_id AS user_id,
    o.item_id AS item_id
FROM
    {{ ref('int_applicative__booking') }} b
JOIN {{ ref('offer_item_ids') }} o on o.offer_id = b.offer_id
JOIN {{ ref('user_beneficiary') }} eud on eud.user_id = b.user_id
WHERE
    booking_is_cancelled = false