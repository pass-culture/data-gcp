with items_grouping AS (
    SELECT
        offer.offer_id,
        CASE
            WHEN (linked_offers.item_linked_id is not null and offer_product_id is null) THEN linked_offers.item_linked_id
            WHEN (offer.offer_product_id is not null) THEN CONCAT('product-',offer.offer_product_id)
            ELSE CONCAT('offer-',offer.offer_id)
        END as item_id
    FROM {{ ref('offer') }} AS offer
    LEFT JOIN {{ source('analytics','linked_offers') }} linked_offers ON linked_offers.offer_id = offer.offer_id
)
SELECT
    offer_id,
    MAX(item_id) as item_id,
FROM
    items_grouping
WHERE
    offer_id is not null
    AND item_id is not null
GROUP BY
    offer_id

