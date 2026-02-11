{{ config(materialized="view") }}

{% set config = get_semantic_embedding_feature_config() %}

{% set all_offer_cols = config.offer.embedding_features + config.offer.extra_data %}
{% set all_metadata_cols = (
    config.offer_metadata.embedding_features
    + config.offer_metadata.extra_data
) %}

with
    items_to_process as (
        select
            items.item_id,
            {% for col in (all_offer_cols + all_metadata_cols) -%}
                items.{{ col }},
            {% endfor -%}
            items.content_hash,
            (
                past.item_id is null
                or (items.content_hash != coalesce(past.content_hash, ''))
            ) as to_embed
        from {{ ref("ml_input__item_metadata") }} as items
        left join
            {{ source("ml_preproc", "item_embedding_extraction") }} as past
            on items.item_id = past.item_id
    )

select
    item_id,
    offer_subcategory_id as subcategory_id,
    offer_category_id as category_id,
    offer_name,
    offer_description,
    image_url as image,
    offer_creation_date,
    content_hash,
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
    trim(concat(coalesce(author, ''), ' ', coalesce(performer, ''))) as author_concat

from items_to_process

order by offer_creation_date desc
