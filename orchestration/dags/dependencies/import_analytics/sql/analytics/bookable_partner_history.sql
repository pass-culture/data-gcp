WITH all_bookable_data AS (
SELECT
    CASE
        WHEN venue_is_permanent THEN CONCAT('venue-',enriched_offer_data.venue_id)
        WHEN NOT is_territorial_authorities THEN CONCAT('offerer-', enriched_offer_data.offerer_id)
         END AS partner_id
    , partition_date
    , 'individual' AS offer_type
    , COUNT(DISTINCT offer_id) AS nb_bookable_offers
FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data
INNER JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data USING(venue_id)
INNER JOIN `{{ bigquery_analytics_dataset }}`.enriched_offerer_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
INNER JOIN `{{ bigquery_analytics_dataset }}`.bookable_offer_history USING(offer_id)
GROUP BY 1,2,3
UNION ALL
SELECT
    CASE
        WHEN venue_is_permanent THEN CONCAT('venue-',enriched_offer_data.venue_id)
        WHEN NOT is_territorial_authorities THEN CONCAT('offerer-', enriched_offer_data.offerer_id)
         END AS partner_id
    , partition_date
    , 'collective' AS offer_type
    , COUNT(DISTINCT collective_offer_id) AS nb_bookable_offers
FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data
INNER JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data USING(venue_id)
INNER JOIN `{{ bigquery_analytics_dataset }}`.enriched_offerer_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
INNER JOIN `{{ bigquery_analytics_dataset }}`.bookable_collective_offer_history USING(collective_offer_id)
GROUP BY 1,2,3),

pivoted_data AS (
SELECT
    partner_id
    , partition_date
    , individual AS  individual_bookable_offers
    , collective AS collective_bookable_offers
FROM all_bookable_data
PIVOT(SUM(nb_bookable_offers) FOR offer_type IN ('individual' , 'collective'))
)

SELECT
    partner_id
    , partition_date
    , COALESCE(individual_bookable_offers, 0) AS individual_bookable_offers
    , COALESCE(collective_bookable_offers, 0) AS collective_bookable_offers
     , COALESCE(individual_bookable_offers, 0) + COALESCE(collective_bookable_offers, 0) AS total_bookable_offers
FROM pivoted_data