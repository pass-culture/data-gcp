with recommendable_offers as (
    select
        *
    from
        `{{ bigquery_analytics_dataset }}.recommendable_offers_data`
),
non_recommendable_offers as (
    select
        *
    from
        `{{ bigquery_analytics_dataset }}.recommendable_offers_data` offer
    Where
        (offer.subcategory_id = 'ACHAT_INSTRUMENT' and REGEXP_CONTAINS(LOWER(offer.name),r'bon d’achat|bons d’achat'))
        OR (offer.subcategory_id = 'MATERIEL_ART_CREATIF'AND REGEXP_CONTAINS(LOWER(offer.name),r'stabilo|surligneurs'))
        OR offer.product_id in (SELECT * FROM `{{ bigquery_raw_dataset }}.forbidden_offers_recommendation`)
)
select
    CURRENT_DATE as date,
    count(*) as recommendable_offers_count,
    (
        SELECT
            count(*) <= 0
        from
            non_recommendable_offers nro
    ) as non_recommenadable_offers_check
from
    recommendable_offers ro