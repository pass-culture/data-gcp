with
    base as (
        select
            o.offer_id,
            case
                when (o.offer_name is null or o.offer_name = 'NaN')
                then "None"
                else safe_cast(o.offer_name as string)
            end as offer_name,
            case
                when (o.offer_description is null or o.offer_description = 'NaN')
                then "None"
                else safe_cast(o.offer_description as string)
            end as offer_description,
            o.offer_validation,
            o.offer_subcategoryid,
            oed.rayon,
            rayon_ref.macro_rayon as macro_rayon,
            case
                when s.stock_price is null
                then 0
                else safe_cast(s.stock_price as integer)
            end as stock_price,
            case
                when
                    subcat.id = 'ESCAPE_GAME'
                    and o.offer_creation_date < datetime '2022-02-01'
                then false
                when
                    subcat.id = 'BON_ACHAT_INSTRUMENT'
                    and o.offer_creation_date < datetime '2022-09-01'
                then false
                else true
            end as is_rule_up_to_date
        from `{{ bigquery_raw_dataset }}`.`applicative_database_offer` o
        left join
            `{{ bigquery_raw_dataset }}`.`applicative_database_stock` s
            on s.offer_id = o.offer_id  -- TODO:update join with offer_extra_data
        left join
            `{{ bigquery_int_applicative_dataset }}`.`extract_offer` oed
            on oed.offer_id = o.offer_id
        left join
            `{{ bigquery_raw_dataset }}`.`subcategories` subcat
            on subcat.id = o.offer_subcategoryid
        left join
            `{{ bigquery_seed_dataset }}`.`macro_rayons` as rayon_ref
            on oed.rayon = rayon_ref.rayon
        where
            o.offer_validation <> 'DRAFT'
            and o.offer_last_validation_type = 'MANUAL'
            and ((o.offer_name is not null or o.offer_name <> 'NaN'))
            and o.offer_creation_date > datetime '2022-09-01'
        group by
            o.offer_id,
            o.offer_name,
            o.offer_description,
            o.offer_validation,
            o.offer_subcategoryid,
            subcat.id,
            o.offer_creation_date,
            oed.rayon,
            macro_rayon,
            stock_price
    )
select
    b.* except (is_rule_up_to_date),
    ie.semantic_content_embedding as semantic_content_embedding,
    ie.image_embedding as image_embedding
from base b
left join
    `{{ bigquery_int_applicative_dataset }}`.offer_item_id oii
    on b.offer_id = oii.offer_id
join `{{ bigquery_ml_feat_dataset }}`.item_embedding ie on oii.item_id = ie.item_id
where is_rule_up_to_date
qualify row_number() over (partition by ie.item_id order by ie.extraction_date desc) = 1
