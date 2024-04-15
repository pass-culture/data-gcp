with base as (
    SELECT
        o.offer_id,
        CASE	
            when (	
                o.offer_name is null	
                or o.offer_name = 'NaN'	
            ) then "None"	
            else safe_cast(o.offer_name as STRING)	
        END as offer_name,	
        CASE	
            when (	
                o.offer_description is null	
                or o.offer_description = 'NaN'	
            ) then "None"	
            else safe_cast(o.offer_description as STRING)	
        END as offer_description,
        o.offer_validation,
        o.offer_subcategoryid,
        oed.rayon,
        rayon_ref.macro_rayon as macro_rayon,
        CASE
            when s.stock_price is null then 0
            else safe_cast(s.stock_price as INTEGER)
        END as stock_price,
        CASE
            WHEN subcat.id = 'ESCAPE_GAME'
            AND o.offer_creation_date < DATETIME '2022-02-01' THEN False
            WHEN subcat.id = 'BON_ACHAT_INSTRUMENT'
            AND o.offer_creation_date < DATETIME '2022-09-01' THEN False
            ELSE True
        END AS is_rule_up_to_date
    FROM
        `{{ bigquery_raw_dataset }}`.`applicative_database_offer` o
        LEFT JOIN `{{ bigquery_raw_dataset }}`.`applicative_database_stock` s on s.offer_id = o.offer_id --TODO:update join with offer_extra_data
        LEFT JOIN `{{ bigquery_clean_dataset }}`.`offer_extracted_data` oed ON oed.offer_id = o.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.`subcategories` subcat ON subcat.id = o.offer_subcategoryid
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.`macro_rayons` AS rayon_ref ON oed.rayon = rayon_ref.rayon
    where
        o.offer_validation <> 'DRAFT'
        and o.offer_last_validation_type = 'MANUAL'
        and (
            (
                o.offer_name is not null
                or o.offer_name <> 'NaN'
            )
        )
        and o.offer_creation_date > DATETIME '2022-09-01'
    GROUP BY
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
    b.*
except
(is_rule_up_to_date),
    ie.semantic_content_embedding as semantic_content_embedding,
    ie.image_embedding as image_embedding
from
    base b
    LEFT JOIN `{{ bigquery_clean_dataset }}`.offer_item_ids oii on b.offer_id = oii.offer_id
    JOIN `{{ bigquery_clean_dataset }}`.item_embeddings ie on oii.item_id = ie.item_id
where
    is_rule_up_to_date
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ie.item_id
    ORDER BY
    ie.extraction_date DESC
) = 1