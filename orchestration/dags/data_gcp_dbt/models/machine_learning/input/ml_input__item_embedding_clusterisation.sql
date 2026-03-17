{{ config(materialized="view") }}

with
    items_w_embedding as (
        select item_id, semantic_content_sts as hybrid_embedding
        from {{ ref("ml_feat__item_embedding_refactor") }}
    )

select
    ie.item_id,
    ie.hybrid_embedding,
    im.offer_subcategory_id as subcategory_id,
    im.offer_category_id as category,
    im.offer_type_id,
    im.offer_type_label,
    im.offer_sub_type_id,
    im.offer_sub_type_label
from items_w_embedding as ie
inner join {{ ref("ml_input__item_metadata") }} as im on ie.item_id = im.item_id
