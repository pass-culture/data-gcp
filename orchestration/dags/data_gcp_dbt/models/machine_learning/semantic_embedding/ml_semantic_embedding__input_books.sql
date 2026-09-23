{{ config(materialized="view") }}

-- Books subset of the semantic base. Filtering lives here (dbt owns it), so
-- the job's books_content.yaml has no category filter.
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
from {{ ref("ml_semantic_embedding__input_all_items") }}
where
    starts_with(item_id, 'product')
    and category_id = 'LIVRE'
    and subcategory_id = 'LIVRE_PAPIER'
