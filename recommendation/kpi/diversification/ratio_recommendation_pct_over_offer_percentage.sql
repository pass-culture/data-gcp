WITH offers AS (
    SELECT
    type, COUNT(*) AS Number_of_offers, SUM(COUNT(*)) OVER () AS Total_number_of_offers, ROUND(COUNT(*) / SUM(COUNT(*)) OVER (),4) AS Percentage_of_offer_types
    FROM `pass-culture-app-projet-test.data_analytics.offer`
    GROUP BY type
    ORDER BY Number_of_offers DESC
),
recommendations AS (
    SELECT
    type, COUNT(*) AS Number_of_recommended_offers, SUM(COUNT(*)) OVER () AS Total_number_of_recommended_offers, ROUND(COUNT(*) / SUM(COUNT(*)) OVER (),4) AS Percentage_of_recommended_offer_types
    FROM `pass-culture-app-projet-test.data_analytics.offer` AS o
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers` AS ro ON CAST(ro.offerid AS STRING)=o.id
    GROUP BY type
    ORDER BY Number_of_recommended_offers DESC
)
SELECT o.type, ROUND(Percentage_of_recommended_offer_types / Percentage_of_offer_types, 2) AS recommendation_pct_over_offer_pct_ratio
FROM offers o
INNER JOIN recommendations r
ON o.type = r.type;
