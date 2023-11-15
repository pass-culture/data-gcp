WITH active_venues_last_30days AS (SELECT
    venue_managing_offerer_id AS offerer_id
    ,STRING_AGG(DISTINCT CONCAT(' ',CASE WHEN venue_type_label != 'Offre numérique' THEN venue_type_label END)) AS active_last_30days_physical_venues_types
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.bookable_venue_history ON enriched_venue_data.venue_id = bookable_venue_history.venue_id
WHERE DATE_DIFF(CURRENT_DATE,partition_date,DAY) <= 30
GROUP BY 1
ORDER BY 1)

,administrative_venues AS (SELECT
    venue_managing_offerer_id AS offerer_id
    ,COUNT(CASE WHEN enriched_venue_data.venue_type_label = 'Lieu administratif' THEN venue_id ELSE NULL END) AS nb_administrative_venues
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
GROUP BY 1)

,top_CA_venue AS
(SELECT
    venue_managing_offerer_id AS offerer_id
    ,enriched_venue_data.venue_type_label
    ,RANK() OVER(PARTITION BY venue_managing_offerer_id ORDER BY real_revenue DESC) AS CA_rank
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
WHERE real_revenue > 0)

,top_bookings_venue AS
(SELECT
    venue_managing_offerer_id AS offerer_id
    ,enriched_venue_data.venue_type_label
    ,RANK() OVER(PARTITION BY venue_managing_offerer_id ORDER BY used_bookings DESC) AS bookings_rank
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
WHERE used_bookings > 0)

,reimbursement_points AS (
SELECT
    venue_managing_offerer_id AS offerer_id
    ,COUNT(DISTINCT applicative_database_venue_reimbursement_point_link.venue_id) AS nb_reimbursement_points
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue_reimbursement_point_link ON enriched_venue_data.Venue_id = applicative_database_venue_reimbursement_point_link.venue_id
GROUP BY 1)

,aggregated_venue_types AS (
    SELECT
        venue_managing_offerer_id AS offerer_id
        ,STRING_AGG(DISTINCT CONCAT(' ',CASE WHEN enriched_venue_data.venue_type_label != 'Offre numérique' THEN enriched_venue_data.venue_type_label END)) AS all_physical_venues_types
    FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
    GROUP BY 1
)

SELECT DISTINCT
    enriched_offerer_data.offerer_id
    ,REPLACE(enriched_offerer_data.partner_id,'offerer','local-authority') AS local_authority_id
    ,enriched_offerer_data.offerer_name AS local_authority_name
    ,CASE
        WHEN (LOWER(enriched_offerer_data.offerer_name) LIKE 'commune%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%ville%de%') THEN 'Communes'
        WHEN (LOWER(enriched_offerer_data.offerer_name) LIKE '%departement%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%département%') THEN 'Départements'
        WHEN (LOWER(enriched_offerer_data.offerer_name) LIKE 'region%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE 'région%') THEN 'Régions'
        WHEN (LOWER(enriched_offerer_data.offerer_name) LIKE 'ca%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%agglo%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE 'cc%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE 'cu%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%communaute%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%agglomeration%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%agglomération%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%metropole%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%com%com%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%petr%'
            OR LOWER(enriched_offerer_data.offerer_name) LIKE '%intercommunal%') THEN 'CC / Agglomérations / Métropoles'
        ELSE 'Non qualifiable' END AS local_authority_type
    --,CASE WHEN enriched_offerer_data.offerer_id IN (SELECT priority_offerer_id FROM `{{ bigquery_analytics_dataset }}`.priority_local_authorities) THEN TRUE ELSE FALSE END AS is_priority
    ,COALESCE(applicative_database_offerer.offerer_validation_date,applicative_database_offerer.offerer_creation_date) AS local_authority_creation_date
    ,CASE WHEN DATE_TRUNC(COALESCE(enriched_offerer_data.offerer_validation_date,enriched_offerer_data.offerer_creation_date),YEAR) <= DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE/*'{{ ds }}'*/),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE FALSE END AS was_registered_last_year
    ,academy_name AS local_authority_academy_name
    ,enriched_offerer_data.offerer_region_name AS local_authority_region_name
    ,enriched_offerer_data.offerer_department_code AS local_authority_department_code
    ,applicative_database_offerer.offerer_postal_code AS local_authority_postal_code
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,offerer_last_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,offerer_last_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_active_current_year
    ,total_venues_managed
    ,physical_venues_managed
    ,permanent_venues_managed
    ,CASE WHEN nb_administrative_venues >= 1 THEN TRUE ELSE FALSE END AS has_administrative_venue
    ,all_physical_venues_types
    ,active_last_30days_physical_venues_types
    ,top_bookings_venue.venue_type_label AS top_bookings_venue_type
    ,top_CA_venue.venue_type_label AS top_CA_venue_type
    ,nb_reimbursement_points
    ,COALESCE(offerer_individual_offers_created,0) AS individual_offers_created
    ,COALESCE(offerer_collective_offers_created,0) AS collective_offers_created
    ,COALESCE(offerer_individual_offers_created,0)+COALESCE(offerer_collective_offers_created,0) AS total_offers_created
    ,offerer_first_offer_creation_date AS first_offer_creation_date
    ,offerer_last_bookable_offer_date AS last_bookable_offer_date
    ,offerer_first_bookable_offer_date AS first_bookable_offer_date
    ,COALESCE(offerer_non_cancelled_individual_bookings,0) AS non_cancelled_individual_bookings
    ,COALESCE(offerer_used_individual_bookings,0) AS used_individual_bookings
    ,COALESCE(offerer_non_cancelled_collective_bookings,0) AS confirmed_collective_bookings
    ,COALESCE(offerer_used_collective_bookings,0) AS used_collective_bookings
    ,COALESCE(offerer_individual_real_revenue,0) AS individual_real_revenue
    ,COALESCE(offerer_collective_real_revenue,0) AS collective_real_revenue
    ,COALESCE(offerer_real_revenue,0) AS total_real_revenue
FROM `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer ON enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
JOIN `{{ bigquery_analytics_dataset }}`.region_department ON enriched_offerer_data.offerer_department_code = region_department.num_dep
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
LEFT JOIN aggregated_venue_types ON enriched_offerer_data.offerer_id = aggregated_venue_types.offerer_id
LEFT JOIN active_venues_last_30days ON enriched_offerer_data.offerer_id = active_venues_last_30days.offerer_id
LEFT JOIN administrative_venues ON enriched_offerer_data.offerer_id = administrative_venues.offerer_id
LEFT JOIN top_CA_venue ON enriched_offerer_data.offerer_id = top_CA_venue.offerer_id
    AND CA_rank = 1
LEFT JOIN top_bookings_venue ON enriched_offerer_data.offerer_id = top_bookings_venue.offerer_id
    AND bookings_rank = 1
LEFT JOIN reimbursement_points ON enriched_offerer_data.offerer_id = reimbursement_points.offerer_id
WHERE is_local_authority IS TRUE