SELECT 
    distinct item_meta.item_id
    , item_meta.offer_type_domain
    , item_meta.titelive_gtl_id
    , item_meta.gtl_type
    , item_meta.gtl_label_level_1
    , item_meta.gtl_label_level_2
    , item_meta.gtl_label_level_3
    , item_meta.gtl_label_level_4
    , item_meta.category_id
    , item_meta.subcategory_id
    , item_meta.offer_type_label
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
    , emb.semantic_content_embedding
    , emb32.semantic_content_embedding as semantic_content_embedding32
    , emb5.semantic_content_embedding as semantic_content_embedding5
FROM {{ ref('item_metadata') }} as  item_meta
LEFT JOIN {{ ref('item_clusters_topics_labels') }} as ictl
    ON item_meta.item_id = ictl.item_id
LEFT JOIN {{ source('clean','item_embeddings') }} as emb
    ON item_meta.item_id = emb.item_id
LEFT JOIN {{ source('clean','item_embeddings_reduced_32') }} as emb32
    ON item_meta.item_id = emb32.item_id
LEFT JOIN {{ source('clean','item_embeddings_reduced_5') }} as emb5
    ON item_meta.item_id = emb5.item_id