{{ config(materialized="view") }}

select
    im.item_id,
    im.offer_subcategory_id as subcategory_id,
    im.offer_category_id as category_id,
    im.offer_name,
    im.offer_description,
    im.image_url as image,
    im.offer_creation_date,
    case
        when im.titelive_gtl_id is not null
        then
            trim(
                concat(
                    coalesce(im.gtl_label_level_1, ''),
                    ' ',
                    coalesce(im.gtl_label_level_2, ''),
                    ' ',
                    coalesce(im.gtl_label_level_3, ''),
                    ' ',
                    coalesce(im.gtl_label_level_4, '')
                )
            )
        when im.offer_type_label is not null
        then trim(array_to_string(im.offer_type_labels, ' '))
    end as offer_label_concat,
    trim(
        concat(coalesce(im.author, ''), ' ', coalesce(im.performer, ''))
    ) as author_concat

from {{ ref("ml_input__item_metadata") }} as im

where im.to_embed = true

order by im.offer_creation_date desc
