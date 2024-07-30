WITH offer_booking_information_view AS (
    SELECT
        offer.offer_id,
        COUNT(DISTINCT booking.booking_id) AS count_booking
    FROM
        {{ ref('offer') }} AS offer
    LEFT JOIN {{ source('raw', 'applicative_database_stock') }} AS stock ON stock.offer_id = offer.offer_id
    LEFT JOIN {{ ref('booking') }} AS booking ON stock.stock_id = booking.stock_id
    WHERE booking_is_used
    GROUP BY
        offer_id
),

enriched_items AS (
    SELECT 
        offer.*,
        offer_ids.item_id,
        IF(offer_type_label is not null, count_booking, null) as count_booking
    FROM {{ ref('offer_metadata') }} offer
    INNER JOIN {{ ref('offer_item_ids') }} offer_ids on offer.offer_id=offer_ids.offer_id
    LEFT JOIN offer_booking_information_view obi on obi.offer_id = offer.offer_id
)

SELECT * except(count_booking, offer_id)
FROM enriched_items
WHERE item_id is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY count_booking DESC) = 1
