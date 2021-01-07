WITH offers AS (
    SELECT
    type, COUNT(*) AS Number_of_offers, SUM(COUNT(*)) OVER () AS Total_number_of_offers, ROUND(COUNT(*) / SUM(COUNT(*)) OVER (),4) AS Percentage_of_offer_types
    FROM `pass-culture-app-projet-test.applicative_database.offer`
    GROUP BY type
    ORDER BY Number_of_offers DESC
),
recommendations AS (
    SELECT
    type, COUNT(*) AS Number_of_recommended_offers, SUM(COUNT(*)) OVER () AS Total_number_of_recommended_offers, ROUND(COUNT(*) / SUM(COUNT(*)) OVER (),4) AS Percentage_of_recommended_offer_types
    FROM `pass-culture-app-projet-test.applicative_database.offer` AS o
    JOIN `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers` AS ro ON CAST(ro.offerid AS STRING)=o.id
    GROUP BY type
    ORDER BY Number_of_recommended_offers DESC
)
select o.type, ROUND(Percentage_of_recommended_offer_types / Percentage_of_offer_types, 2) AS recommendation_pct_over_offer_pct_ratio
from offers o
left join recommendations r
on o.type = r.type;
