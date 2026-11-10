{{ config(materialized="view") }}

select
    base.item_id,
    base.subcategory_id,
    base.category_id,
    base.offer_name,
    base.offer_description,
    base.image,
    base.offer_creation_date,
    base.content_hash,
    {{ to_embed_condition("base", "past") }} as to_embed,
    base.author_concat,
    -- non-null and non-empty Titelive GTL levels joined as a flat comma-separated list.
    nullif(
        array_to_string(
            array(
                select level
                from
                    unnest(
                        [
                            struct(trim(base.gtl_label_level_1) as level),
                            struct(trim(base.gtl_label_level_2) as level),
                            struct(trim(base.gtl_label_level_3) as level),
                            struct(trim(base.gtl_label_level_4) as level)
                        ]
                    )
                where level is not null and level != ''
            ),
            ', '
        ),
        ''
    ) as gtl_concat
from {{ ref("ml_input__all_items_metadata_to_embed") }} as base
left join
    {{ ref("ml_semantic_embedding__books_metadata") }} as past
    on base.item_id = past.item_id
where
    starts_with(base.item_id, 'product')
    and base.category_id = 'LIVRE'
    and base.subcategory_id = 'LIVRE_PAPIER'
