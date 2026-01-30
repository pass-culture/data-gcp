with
    base as (
        select
            offer.offer_id,
            case
                when (offer.offer_name is null or offer.offer_name = 'NaN')
                then "None"
                else safe_cast(offer.offer_name as string)
            end as offer_name,
            case
                when
                    (offer.offer_description is null or offer.offer_description = 'NaN')
                then "None"
                else safe_cast(offer.offer_description as string)
            end as offer_description,
            offer.offer_validation,
            offer.offer_subcategoryid as offer_subcategory_id,
            extract_offer.rayon,
            macro_rayons.macro_rayon as macro_rayon,
            case
                when stock.stock_price is null
                then 0
                else safe_cast(stock.stock_price as integer)
            end as stock_price,
            case
                when
                    subcategories.id = 'ESCAPE_GAME'
                    and offer.offer_creation_date < datetime '2022-02-01'
                then false
                when
                    subcategories.id = 'BON_ACHAT_INSTRUMENT'
                    and offer.offer_creation_date < datetime '2022-09-01'
                then false
                else true
            end as is_rule_up_to_date
        from {{ ref("int_raw__offer") }} offer
        left join
            {{ source("raw", "applicative_database_stock") }} stock
            on stock.offer_id = offer.offer_id  -- TODO:update join with offer_extra_data
        left join
            {{ ref("int_applicative__extract_offer") }} extract_offer
            on extract_offer.offer_id = offer.offer_id
        left join
            {{ source("raw", "subcategories") }} subcategories
            on subcategories.id = offer.offer_subcategoryid
        left join
            {{ source("seed", "macro_rayons") }} as macro_rayons
            on extract_offer.rayon = macro_rayons.rayon
        where
            offer.offer_validation <> 'DRAFT'
            and offer.offer_last_validation_type = 'MANUAL'
            and ((offer.offer_name is not null or offer.offer_name <> 'NaN'))
            and offer.offer_creation_date > datetime '2022-09-01'
        group by
            offer.offer_id,
            offer.offer_name,
            offer.offer_description,
            offer.offer_validation,
            offer.offer_subcategoryid,
            subcategories.id,
            offer.offer_creation_date,
            extract_offer.rayon,
            macro_rayon,
            stock_price
    )
select
    base.* except (is_rule_up_to_date),
    item_embedding.semantic_content_embedding as semantic_content_embedding,
    item_embedding.image_embedding as image_embedding
from base
left join
    {{ ref("int_applicative__offer_item_id") }} as offer_item_id
    on base.offer_id = offer_item_id.offer_id

join
    {{ ref("ml_feat__item_embedding_v2") }} item_embedding
    on offer_item_id.item_id = item_embedding.item_id
where is_rule_up_to_date
qualify
    row_number() over (
        partition by item_embedding.item_id order by item_embedding.extraction_date desc
    )
    = 1
