WITH offers AS (
    SELECT
    offer_type, COUNT(*) AS Number_of_offers, SUM(COUNT(*)) OVER () AS Total_number_of_offers, ROUND(COUNT(*) / SUM(COUNT(*)) OVER (),4) AS Percentage_of_offer_types
    FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
    GROUP BY offer_type
    ORDER BY Number_of_offers DESC
),
recommendations AS (
    SELECT
    offer_type, COUNT(*) AS Number_of_recommended_offers, SUM(COUNT(*)) OVER () AS Total_number_of_recommended_offers, ROUND(COUNT(*) / SUM(COUNT(*)) OVER (),4) AS Percentage_of_recommended_offer_types
    FROM `passculture-data-prod.analytics_prod.applicative_database_offer` AS o
    INNER JOIN `passculture-data-prod.raw_prod.past_recommended_offers` AS ro ON CAST(ro.offerid AS STRING)=o.offer_id
    WHERE ro.date >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND ro.date < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)          -- pour gérer la période d'AB testing
    GROUP BY offer_type
    ORDER BY Number_of_recommended_offers DESC
)
SELECT o.offer_type, ROUND(Percentage_of_recommended_offer_types / Percentage_of_offer_types, 2) AS recommendation_pct_over_offer_pct_ratio
FROM offers o
INNER JOIN recommendations r
ON o.offer_type = r.offer_type;
