{{ config(materialized="view") }}

with
    last_extraction as (
        select distinct item_id
        from {{ source("ml_preproc", "item_embedding_extraction_v2") }}
        where date(extraction_date) > date_sub(current_date, interval 30 day)
    )

select
    im.item_id,
    im.offer_subcategory_id as subcategory_id,
    im.offer_category_id as category_id,
    im.offer_name,
    im.offer_description,
    im.image_url as image,
    case
        when titelive_gtl_id is not null
        then
            trim(
                concat(
                    coalesce(gtl_label_level_1, ''),
                    ' ',
                    coalesce(gtl_label_level_2, ''),
                    ' ',
                    coalesce(gtl_label_level_3, ''),
                    ' ',
                    coalesce(gtl_label_level_4, '')
                )
            )
        when offer_type_label is not null
        then trim(array_to_string(offer_type_labels, ' '))
    end as offer_label_concat,
    trim(concat(coalesce(author, ''), ' ', coalesce(performer, ''))) as author_concat,
    offer_creation_date

from {{ ref("ml_input__item_metadata") }} im
left join last_extraction le on le.item_id = im.item_id
where le.item_id is null

order by offer_creation_date desc
