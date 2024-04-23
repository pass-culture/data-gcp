WITH items_w_embedding as (
    SELECT
        ie.item_id,
        ie.hybrid_embedding as hybrid_embedding,
    FROM
        `{{ bigquery_clean_dataset }}.item_embeddings_reduced_16` ie
),
base as (
    SELECT
        ie.item_id,
        ie.hybrid_embedding,
        im.subcategory_id AS subcategory_id,
        im.category_id as category,
        im.offer_type_id,
        im.offer_type_label,
        im.offer_sub_type_id,
        im.offer_sub_type_label,
    FROM items_w_embedding ie
    LEFT JOIN `{{ bigquery_clean_dataset }}.item_metadata` im on ie.item_id = im.item_id
)
select
    *
from
    base