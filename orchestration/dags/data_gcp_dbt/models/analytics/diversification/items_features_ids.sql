SELECT 
    distinct offer_item.item_id
    , offer_meta.titelive_gtl_id
    , offer_meta.gtl_label_level_1
    , offer_meta.gtl_label_level_2
    , offer_meta.gtl_label_level_3
    , offer_meta.gtl_label_level_4
    , ictl.semantic_cluster_id
    , ictl.semantic_category
    , ictl.topic_id
    , ictl.category_lvl0
    , ictl.category_lvl1
    , ictl.category_medium_lvl1
    , ictl.category_genre_lvl1
    , ictl.category_lvl2
    , ictl.category_medium_lvl2
    , ictl.category_genre_lvl2
    , ictl.micro_category_details
    , ictl.macro_category_details
    , offer_meta.subcategory_id
    , offer_meta.category_id
    , offer_meta.offer_type_label
    , emb.semantic_content_embedding
    , emb32.semantic_content_embedding as semantic_content_embedding32
    , emb5.semantic_content_embedding as semantic_content_embedding5
FROM {{ ref('offer_metadata') }} as  offer_meta
LEFT JOIN {{ ref('offer_item_ids') }} as offer_item
    ON  offer_meta.offer_id = offer_item.offer_id
LEFT JOIN {{ ref('item_clusters_topics_labels') }} as ictl
    ON offer_item.item_id = ictl.item_id
LEFT JOIN {{ source('clean','item_embeddings') }} as emb
    ON offer_item.item_id = emb.item_id
LEFT JOIN {{ source('clean','item_embeddings_reduced_32') }} as emb32
    ON emb.item_id = emb32.item_id
LEFT JOIN {{ source('clean','item_embeddings_reduced_5') }} as emb5
    ON emb.item_id = emb5.item_id