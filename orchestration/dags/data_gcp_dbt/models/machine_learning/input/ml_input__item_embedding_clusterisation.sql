{{
    config(
        materialized = "view"
    )
}}

WITH items_w_embedding as (
    SELECT
        item_id,
        hybrid_embedding as hybrid_embedding,
    FROM
        {{ source("ml_preproc", "item_embedding_reduced_32") }}
)

SELECT
    ie.item_id,
    ie.hybrid_embedding,
    im.subcategory_id AS subcategory_id,
    im.category_id as category,
    im.offer_type_id,
    im.offer_type_label,
    im.offer_sub_type_id,
    im.offer_sub_type_label
FROM items_w_embedding ie
INNER JOIN {{ ref("item_metadata") }} im on ie.item_id = im.item_id
