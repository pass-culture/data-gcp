WITH
permanent_venues AS (SELECT
    enriched_venue_data.venue_id AS venue_id
    ,venue_managing_offerer_id AS offerer_id
    ,venue_creation_date AS partner_creation_date
    ,CASE WHEN DATE_TRUNC(venue_creation_date,YEAR) <= DATE_TRUNC(DATE_SUB(DATE('{{ ds }}'),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE NULL END AS was_registered_last_year
    ,enriched_venue_data.venue_name AS partner_name
    ,region_department.academy_name AS partner_academy_name
    ,enriched_venue_data.venue_region_name AS partner_region_name
    ,enriched_venue_data.venue_department_code AS partner_department_code
    ,enriched_venue_data.venue_postal_code AS partner_postal_code
    ,'venue' AS partner_status
    ,venue_type_label AS partner_type
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,venue_last_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,venue_last_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_active_current_year
    ,COALESCE(enriched_venue_data.collective_offers_created,0) AS collective_offers_created
    ,COALESCE(enriched_venue_data.individual_offers_created,0) AS individual_offers_created
    ,(COALESCE(enriched_venue_data.collective_offers_created,0) + COALESCE(enriched_venue_data.individual_offers_created,0)) AS total_offers_created
    ,venue_last_bookable_offer_date AS last_bookable_offer_date
    , venue_first_bookable_offer_date AS first_bookable_offer_date
    ,COALESCE(enriched_venue_data.non_cancelled_individual_bookings,0) AS non_cancelled_individual_bookings
    ,COALESCE(enriched_venue_data.used_individual_bookings,0) AS used_individual_bookings
    ,COALESCE(enriched_venue_data.non_cancelled_collective_bookings,0) AS confirmed_collective_bookings
    ,COALESCE(enriched_venue_data.used_collective_bookings,0) AS used_collective_bookings
    ,COALESCE(enriched_venue_data.individual_real_revenue,0) AS real_individual_revenue
    ,COALESCE(enriched_venue_data.collective_real_revenue,0) AS real_collective_revenue
    ,(COALESCE(enriched_venue_data.individual_real_revenue,0)+COALESCE(enriched_venue_data.collective_real_revenue,0)) AS total_real_revenue
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data AS enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS region_department
    ON enriched_venue_data.venue_department_code = region_department.num_dep
WHERE venue_is_permanent IS TRUE),

tagged_partners AS (
SELECT
    offerer_id
    ,STRING_AGG(DISTINCT (CASE WHEN tag_label IS NOT NULL THEN tag_label ELSE NULL END) ORDER BY (CASE WHEN tag_label IS NOT NULL THEN tag_label ELSE NULL END)) AS partner_type
FROM `{{ bigquery_analytics_dataset }}`.enriched_offerer_tags_data
WHERE tag_category_name = 'comptage'
AND tag_label NOT IN ('Association', 'EPN','Collectivité','Pas de tag associé','Auto-Entrepreneur')
GROUP BY 1
)

,offerers AS (
SELECT
    '' AS venue_id
    ,enriched_offerer_data.offerer_id
    ,enriched_offerer_data.offerer_creation_date AS partner_creation_date
    ,CASE WHEN DATE_TRUNC(enriched_offerer_data.offerer_creation_date,YEAR) <= DATE_TRUNC(DATE_SUB(DATE('{{ ds }}'),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE NULL END AS was_registered_last_year
     ,enriched_offerer_data.offerer_name AS partner_name
    ,region_department.academy_name AS partner_academy_name
    ,enriched_offerer_data.offerer_region_name AS partner_region_name
    ,enriched_offerer_data.offerer_department_code AS partner_department_code
    ,applicative_database_offerer.offerer_postal_code AS partner_postal_code
    ,'offerer' AS partner_status
    ,COALESCE(tagged_partners.partner_type, 'Structure non tagguée') AS partner_type
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,enriched_offerer_data.offerer_last_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,enriched_offerer_data.offerer_last_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_active_current_year
    ,COALESCE(enriched_offerer_data.offerer_individual_offers_created,0) AS individual_offers_created
    ,COALESCE(enriched_offerer_data.offerer_collective_offers_created,0) AS collective_offers_created
    ,COALESCE(enriched_offerer_data.offerer_individual_offers_created,0) + COALESCE(enriched_offerer_data.offerer_collective_offers_created,0) AS total_offers_created
    ,enriched_offerer_data.offerer_last_bookable_offer_date AS last_bookable_offer_date
    ,enriched_offerer_data.offerer_first_bookable_offer_date AS first_bookable_offer_date
    , COALESCE(enriched_offerer_data.offerer_non_cancelled_individual_bookings,0) AS non_cancelled_individual_bookings
    , COALESCE(enriched_offerer_data.offerer_used_individual_bookings,0) AS used_individual_bookings
    , COALESCE(enriched_offerer_data.offerer_non_cancelled_collective_bookings,0) AS confirmed_collective_bookings
    , COALESCE(enriched_offerer_data.offerer_used_collective_bookings,0) AS used_collective_bookings
    , COALESCE(enriched_offerer_data.offerer_individual_real_revenue,0) AS real_individual_revenue
    , COALESCE(enriched_offerer_data.offerer_collective_real_revenue,0) AS real_collective_revenue
    , COALESCE(enriched_offerer_data.offerer_individual_real_revenue,0) + COALESCE(enriched_offerer_data.offerer_collective_real_revenue,0) AS total_real_revenue
FROM `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS applicative_database_offerer
    ON enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS region_department
    ON enriched_offerer_data.offerer_department_code = region_department.num_dep
LEFT JOIN tagged_partners ON tagged_partners.offerer_id = enriched_offerer_data.offerer_id
LEFT JOIN permanent_venues ON permanent_venues.offerer_id = enriched_offerer_data.offerer_id
WHERE permanent_venues.offerer_id IS NULL -- Pas déjà compté à l'échelle du lieu permanent
AND (enriched_offerer_data.legal_unit_business_activity_code IS NULL OR enriched_offerer_data.legal_unit_business_activity_code != '84.11Z') -- Pas de collectivité
)

SELECT *
FROM permanent_venues
UNION ALL
SELECT *
FROM offerers