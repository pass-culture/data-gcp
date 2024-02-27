
{{ config( tags="sandbox" ) }}

with unique_offers as (
    SELECT
    offer_id,
    titelive_gtl_id
    from {{ ref('enriched_offer_data') }} 
    qualify row_number() over ( partition by item_id order by booking_cnt desc, booking_confirm_cnt desc, favourite_cnt desc) =1
),
items as (
    SELECT
    offer.offer_id,
    matching.item_id,
    offer.titelive_gtl_id,
    item_metadata.item_id,
    item_metadata.topic_id,
    item_metadata.cluster_id
    FROM unique_offers as offer
    LEFT JOIN {{ ref('offer_item_ids') }} as matching 
        ON offer.offer_id = matching.offer_id
    FULL OUTER JOIN {{ ref('enriched_item_metadata') }} as item_metadata 
        ON item_metadata.item_id = matching.item_id
)
SELECT 
    items.*,
    emb.semantic_content_embedding
FROM items
LEFT JOIN {{ source('clean','item_embeddings') }} as emb
    ON items.item_id = emb.item_id
