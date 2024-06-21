WITH partner_crea_frequency AS (
SELECT
    partner_id
    , COUNT(DISTINCT DATE_TRUNC(offer_creation_date, MONTH)) AS nb_mois_crea_this_year
FROM {{ ref('mrt_global__offer')}}
WHERE DATE_DIFF(current_date, offer_creation_date, MONTH) <= 12
GROUP BY 1
),

cultural_sector_crea_frequency AS (
SELECT DISTINCT
    cultural_sector
    , PERCENTILE_DISC(nb_mois_crea_this_year, 0.5) OVER(PARTITION BY cultural_sector) AS median_crea_offer_frequency
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
    cultural_sector
    , PERCENTILE_DISC(nb_mois_bookable_this_year, 0.5) OVER(PARTITION BY cultural_sector) AS median_bookability_frequency
FROM partner_bookability_frequency
INNER JOIN {{ ref('enriched_cultural_partner_data')}} USING (partner_id)
)

SELECT
    cultural_sector
    , median_bookability_frequency
    , median_crea_offer_frequency
    , CASE
        WHEN median_bookability_frequency <= 6 THEN 1
        WHEN median_bookability_frequency >= 11 THEN 3
        ELSE 2 END AS cultural_sector_bookability_frequency_group
FROM cultural_sector_crea_frequency
LEFT JOIN cultural_sector_bookability_frequency USING(cultural_sector)