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
        enriched_item_metadata.subcategory_id AS subcategory_id,
        enriched_item_metadata.category_id as category,
        enriched_item_metadata.offer_type_id,
        enriched_item_metadata.offer_type_label,
        enriched_item_metadata.offer_sub_type_id,
        enriched_item_metadata.offer_sub_type_label,
    FROM items_w_embedding ie
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_item_metadata` enriched_item_metadata on ie.item_id = enriched_item_metadata.item_id
)
select
    *
from
    base