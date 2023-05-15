{{ create_humanize_id_function() }}
WITH offer_humanized_id AS (
    SELECT
        offer_id,
        humanize_id(offer_id) AS humanized_id,
    FROM
        `raw_dev`.`applicative_database_offer`
    WHERE
        offer_id is not NULL
),
mediation AS (
    SELECT
        offer_id,
        humanize_id(id) as mediation_humanized_id
    FROM
        (
            SELECT
                id,
                offerId as offer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY offerId
                    ORDER BY
                        dateModifiedAtLastProvider DESC
                ) as rnk
            FROM
                `analytics_dev`.`applicative_database_mediation`
            WHERE
                isActive
        ) inn
    WHERE
        rnk = 1
),
base as (
    SELECT
        o.offer_validation,
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
        o.offer_subcategoryid,
        oed.rayon,
        rayon_ref.macro_rayon as macro_rayon,
CASE
            when s.stock_price is null then 0
            else safe_cast(s.stock_price as INTEGER)
        END as stock_price,
CASE
            when s.stock_quantity is null then 0
            else safe_cast(s.stock_quantity as INTEGER)
        END as stock,
CASE
            WHEN mediation.mediation_humanized_id is not null THEN CONCAT(
                "https://storage.googleapis.com/passculture-metier-prod-production-assets-fine-grained/thumbs/mediations/",
                mediation.mediation_humanized_id
            )
            ELSE CONCAT(
                "https://storage.googleapis.com/passculture-metier-prod-production-assets-fine-grained/thumbs/products/",
                humanize_id(o.offer_product_id)
            )
        END AS offer_image,
CASE
            WHEN subcat.category_id <> 'MUSIQUE_LIVE'
            AND oed.showType IS NOT NULL THEN oed.showType
            WHEN subcat.category_id = 'MUSIQUE_LIVE' THEN oed.musicType
            WHEN subcat.category_id <> 'SPECTACLE'
            AND oed.musicType IS NOT NULL THEN oed.musicType
        END AS type,
CASE
            WHEN subcat.category_id <> 'MUSIQUE_LIVE'
            AND oed.showSubType IS NOT NULL THEN oed.showSubType
            WHEN subcat.category_id = 'MUSIQUE_LIVE' THEN oed.musicSubtype
            WHEN subcat.category_id <> 'SPECTACLE'
            AND oed.musicsubType IS NOT NULL THEN oed.musicSubtype
        END AS subType,
CASE
            WHEN subcat.id = 'ESCAPE_GAME'
            AND o.offer_creation_date < DATETIME '2022-02-01' THEN False
            WHEN subcat.id = 'BON_ACHAT_INSTRUMENT'
            AND o.offer_creation_date < DATETIME '2022-09-01' THEN False
            ELSE True
        END AS is_rule_up_to_date
    FROM
        `raw_dev`.`applicative_database_offer` o
        LEFT JOIN `raw_dev`.`applicative_database_stock` s on s.offer_id = o.offer_id 
        --TODO:update join with offer_extra_data
        LEFT JOIN `analytics_dev`.`offer_extracted_data` oed ON oed.offer_id = o.offer_id
        LEFT JOIN `analytics_dev`.`subcategories` subcat ON subcat.id = o.offer_subcategoryid
        LEFT JOIN `analytics_dev`.`macro_rayons` AS rayon_ref ON oed.rayon = rayon_ref.rayon
        LEFT JOIN mediation ON o.offer_id = mediation.offer_id
    where
        o.offer_validation <> 'DRAFT'
        and o.offer_last_validation_type = 'MANUAL'
        and (
            (
                o.offer_name is not null
                or o.offer_name <> 'NaN'
            )
        ) --and o.offer_creation_date>DATETIME '2022-09-01'

)
select
    *
from
    base
where
    is_rule_up_to_date