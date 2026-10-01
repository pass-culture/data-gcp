{{ config(materialized="view") }}

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
    author_concat,
    -- non-null and non-empty Titelive GTL levels joined as a flat comma-separated list.
    nullif(
        array_to_string(
            array(
                select level
                from
                    unnest(
                        [
                            struct(trim(gtl_label_level_1) as level),
                            struct(trim(gtl_label_level_2) as level),
                            struct(trim(gtl_label_level_3) as level),
                            struct(trim(gtl_label_level_4) as level)
                        ]
                    )
                where level is not null and level != ''
            ),
            ', '
        ),
        ''
    ) as gtl_concat
from {{ ref("ml_input__all_items_metadata_to_embed") }}
where
    starts_with(item_id, 'product')
    and category_id = 'LIVRE'
    and subcategory_id = 'LIVRE_PAPIER'
