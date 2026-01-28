{{ config(materialized="view") }}

with
    items_w_embedding as (
        select item_id, hybrid_embedding as hybrid_embedding
        from {{ source("ml_preproc", "item_embedding_v2_reduced_32") }}
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
from items_w_embedding ie
inner join {{ ref("ml_input__item_metadata") }} im on ie.item_id = im.item_id
