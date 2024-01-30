WITH partner_crea_frequency AS (
SELECT
    enriched_offer_data.partner_id
    , COUNT(DISTINCT DATE_TRUNC(offer_creation_date, MONTH)) AS nb_mois_crea_this_year
FROM {{ ref('enriched_offer_data')}}
WHERE DATE_DIFF(current_date, offer_creation_date, MONTH) <= 12
GROUP BY 1
),

cultural_sector_crea_frequency AS (
SELECT DISTINCT
    enriched_cultural_partner_data.partner_type
    , PERCENTILE_DISC(nb_mois_crea_this_year, 0.5) OVER(PARTITION BY enriched_cultural_partner_data.partner_type) AS median_crea_offer_frequency
FROM partner_crea_frequency
INNER JOIN {{ ref('enriched_cultural_partner_data')}} USING (partner_id)
),

partner_bookability_frequency AS (
SELECT
    partner_id
    , COUNT(DISTINCT DATE_TRUNC(partition_date, MONTH)) AS nb_mois_bookable_this_year
FROM {{ ref('bookable_partner_history')}}
WHERE DATE_DIFF(current_date, partition_date, MONTH) <= 12
GROUP BY 1),

cultural_sector_bookability_frequency AS (
SELECT DISTINCT
    enriched_cultural_partner_data.partner_type
    , PERCENTILE_DISC(nb_mois_bookable_this_year, 0.5) OVER(PARTITION BY enriched_cultural_partner_data.partner_type) AS median_bookability_frequency
FROM partner_bookability_frequency
INNER JOIN {{ ref('enriched_cultural_partner_data')}} USING (partner_id)
)

SELECT
    partner_type
    , median_bookability_frequency
    , median_crea_offer_frequency
    , CASE
        WHEN median_bookability_frequency <= 6 THEN 1
        WHEN median_bookability_frequency >= 11 THEN 3
        ELSE 2 END AS cultural_sector_bookability_frequency_group
FROM cultural_sector_crea_frequency
LEFT JOIN cultural_sector_bookability_frequency USING(partner_type)
ORDER BY 1