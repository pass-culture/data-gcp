{{
    config(
        materialized = "table"
    )
}}

SELECT
    DISTINCT b.user_id AS user_id,
    o.item_id AS item_id
FROM
    {{ ref('int_applicative__booking') }} b
-- TODO add join in int_applicative__booking
JOIN  {{ ref('int_applicative__stock') }} s on s.stock_id = b.stock_id
JOIN {{ ref('offer_item_ids') }} o on o.offer_id = s.offer_id
JOIN {{ ref('user_beneficiary') }} eud on eud.user_id = b.user_id
WHERE
    booking_is_cancelled = false