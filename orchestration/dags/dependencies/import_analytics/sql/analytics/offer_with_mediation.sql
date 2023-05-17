with product_w_mediation as(
    SELECT
        cast(product.id as string) as id
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_product product
    where
        product.thumbCount > 0
),
product_w_mediation_offer_ids as (
    select
        pwm.id,
        offer.offer_id as offer_id
    from
        product_w_mediation pwm
        join `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer on offer.offer_product_id = pwm.id
),
offer_w_mediation as(
    select
        mediation.offerId as offer_id
    from
        `{{ bigquery_clean_dataset }}`.applicative_database_mediation mediation
    where
        mediation.isActive
        AND mediation.thumbCount > 0
),
product_and_offer_with_mediation as(
    select
        offer_id
    from
        offer_w_mediation
    UNION
    ALL
    select
        offer_id
    from
        product_w_mediation_offer_ids
)
select
    distinct(offer_id) as offer_id
from
    product_and_offer_with_mediation