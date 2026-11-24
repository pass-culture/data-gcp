{{ config(materialized="view") }}

-- Every item's offer_name alone, with no category filter.
-- Blank names are excluded: prepare drops their empty prompt, so they would
-- otherwise stay to_embed forever.
select
    base.item_id,
    base.offer_name,
    base.content_hash,
    {{ to_embed_condition("base", "past") }} as to_embed
from {{ ref("ml_input__all_items_metadata_to_embed") }} as base
left join
    {{ ref("ml_semantic_embedding__all_items_offer_names") }} as past
    on base.item_id = past.item_id
where nullif(trim(base.offer_name), '') is not null
