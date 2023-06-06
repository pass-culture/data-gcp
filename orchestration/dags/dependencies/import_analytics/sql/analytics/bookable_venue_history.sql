WITH all_bookable_data AS (
SELECT
    venue_id
    , offerer_id
    , partition_date
    , 'individual' AS offer_type
    , COUNT(DISTINCT offer_id) AS nb_bookable_offers
FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data
INNER JOIN `{{ bigquery_analytics_dataset }}`.bookable_offer_history USING(offer_id)
GROUP BY 1,2,3,4
UNION ALL
SELECT
    venue_id
    , offerer_id
    , partition_date
    , 'collective' AS offer_type
    , COUNT(DISTINCT collective_offer_id) AS nb_bookable_offers
FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data
INNER JOIN `{{ bigquery_analytics_dataset }}`.bookable_collective_offer_history USING(collective_offer_id)
GROUP BY 1,2,3,4),

pivoted_data AS (
SELECT
    venue_id
    , offerer_id
    , partition_date
    , individual AS  individual_bookable_offers
    , collective AS collective_bookable_offers
FROM all_bookable_data
PIVOT(SUM(nb_bookable_offers) FOR offer_type IN ('individual' , 'collective'))
)

SELECT
    venue_id
    , offerer_id
    , partition_date
    , COALESCE(individual_bookable_offers, 0) AS individual_bookable_offers
    , COALESCE(collective_bookable_offers, 0) AS collective_bookable_offers
     , COALESCE(individual_bookable_offers, 0) + COALESCE(collective_bookable_offers, 0) AS total_bookable_offers
FROM pivoted_data