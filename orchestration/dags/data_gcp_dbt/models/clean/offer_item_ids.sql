with item_group_by_extra_data as(
    select
        offer.offer_id,
        CASE WHEN (
            offer.offer_subcategoryId = 'LIVRE_PAPIER'
            AND offer_extracted_data.isbn IS NOT NULL
            AND offer_extracted_data.isbn <> ''
            ) THEN CONCAT('isbn-',offer_extracted_data.isbn)
        WHEN (offer.offer_subcategoryId = 'SEANCE_CINE'
            AND offer_extracted_data.theater_movie_id IS NOT NULL
            AND offer_extracted_data.theater_movie_id <> ''
            ) THEN CONCAT('movie_id-',offer_extracted_data.theater_movie_id)
        WHEN (offer.offer_subcategoryId = 'SUPPORT_PHYSIQUE_MUSIQUE_CD'
            AND offer_extracted_data.ean IS NOT NULL
            AND offer_extracted_data.ean <> ''
            ) THEN CONCAT('cd_id-',offer_extracted_data.ean)
        WHEN (offer.offer_subcategoryId = 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
            AND offer_extracted_data.ean IS NOT NULL
            AND offer_extracted_data.ean <> ''
            ) THEN CONCAT('vinyle_id-',offer_extracted_data.ean)
        ELSE CONCAT('offer-',offer.offer_id) END AS item_id
    FROM
        {{ ref('offer') }} AS offer
    LEFT JOIN {{ ref('offer_extracted_data') }} offer_extracted_data ON offer_extracted_data.offer_id = offer.offer_id
),
items_grouping AS (
    SELECT
        offer.offer_id,
        CASE
            WHEN linked_offers.item_linked_id is not null THEN REGEXP_REPLACE(
                linked_offers.item_linked_id,
                r'[^a-zA-Z0-9\-\_]',
                ''
            )
            else REGEXP_REPLACE(offer.item_id, r'[^a-zA-Z0-9\-\_]', '')
        END as item_id
    FROM
        item_group_by_extra_data offer
        LEFT JOIN {{ source('analytics','linked_offers') }} linked_offers ON linked_offers.offer_id = offer.offer_id
)
SELECT
    offer_id,
    MAX(item_id) as item_id
FROM
    items_grouping
WHERE
    offer_id is not null
    AND item_id is not null
GROUP BY
    offer_id
