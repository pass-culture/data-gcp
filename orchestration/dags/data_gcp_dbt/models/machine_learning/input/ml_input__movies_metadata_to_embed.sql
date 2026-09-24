{{ config(materialized="view") }}

-- Movies subset of the semantic base. Filtering lives here (dbt owns it), so
-- the job's movies_metadata.yaml has no category filter.
select
    item_id,
    subcategory_id,
    category_id,
    offer_name,
    offer_description,
    image,
    offer_creation_date,
    content_hash,
    to_embed,
    offer_label_concat,
    author_concat
from {{ ref("ml_input__all_items_metadata_to_embed") }}
where
    starts_with(item_id, 'product')
    and (
        (
            category_id = 'CINEMA'
            and subcategory_id
            in ('SEANCE_CINE', 'CINE_PLEIN_AIR', 'EVENEMENT_CINE', 'FESTIVAL_CINE')
        )
        or (
            category_id = 'FILM'
            and subcategory_id
            in ('SUPPORT_PHYSIQUE_FILM', 'VOD', 'AUTRE_SUPPORT_NUMERIQUE')
        )
    )
