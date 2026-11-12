{{ config(materialized="view") }}

-- Every item's offer_name alone, with no category filter. to_embed compares
-- against this vector's own merged table, not all_items_metadata's.
select
    base.item_id,
    base.offer_name,
    base.content_hash,
    (
        past.item_id is null or base.content_hash != coalesce(past.content_hash, '')
    ) as to_embed
from {{ ref("ml_input__all_items_metadata_to_embed") }} as base
left join
    {{ ref("ml_semantic_embedding__all_items_offer_names") }} as past
    on base.item_id = past.item_id
