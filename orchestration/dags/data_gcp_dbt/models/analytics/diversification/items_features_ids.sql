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
    offer.titelive_gtl_id,
    item_metadata.item_id,
    item_metadata.topic_id,
    item_metadata.cluster_id,
    item_metadata.simple_topic_id,
    item_metadata.simple_cluster_id,
    item_metadata.unconstrained_topic_id,
    item_metadata.unconstrained_cluster_id
    FROM unique_offers as offer
    LEFT JOIN {{ ref('offer_item_ids') }} as matching 
        ON offer.offer_id = matching.offer_id
    FULL OUTER JOIN {{ ref('enriched_item_metadata') }} as item_metadata 
        ON item_metadata.item_id = matching.item_id
)
SELECT 
    items.*,
    ARRAY(
            SELECT
                cast(elem as float64)
            FROM
                UNNEST(
                    SPLIT(
                        SUBSTR(
                            semantic_content_embedding,
                            2,
                            LENGTH(semantic_content_embedding) - 2
                        )
                    )
                ) elem
        ) AS semantic_content_embedding,
FROM items
INNER JOIN {{ source('clean','item_embeddings_reduced_32') }} as emb
    ON items.item_id = emb.item_id
ORDER BY cluster_id