SELECT
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
    ,CASE WHEN enriched_offerer_data.offerer_id IN (SELECT priority_offerer_id FROM `{{ bigquery_analytics_dataset }}`.priority_local_authorities) THEN TRUE ELSE FALSE END AS is_priority
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
    ,COALESCE(offerer_individual_offers_created,0) AS individual_offers_created
    ,COALESCE(offerer_collective_offers_created,0) AS collective_offers_created
    ,COALESCE(offerer_individual_offers_created,0)+COALESCE(offerer_collective_offers_created,0) AS total_offers_created
    ,offerer_first_offer_creation_date AS first_offer_creation_date
    ,offerer_last_bookable_offer_date AS last_bookable_offer_date
    ,offerer_first_bookable_offer_date AS first_bookable_offer_date
    ,COALESCE(offerer_non_cancelled_individual_bookings) AS non_cancelled_individual_bookings
    ,COALESCE(offerer_used_individual_bookings) AS used_individual_bookings
    ,COALESCE(offerer_non_cancelled_collective_bookings) AS confirmed_collective_bookings
    ,COALESCE(offerer_used_collective_bookings) AS used_collective_bookings
    ,COALESCE(offerer_real_revenue) AS individual_real_revenue
    ,COALESCE(offerer_collective_real_revenue) AS collective_real_revenue
    ,COALESCE(offerer_real_revenue)+COALESCE(offerer_collective_real_revenue) AS total_real_revenue
FROM `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer ON enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
JOIN `{{ bigquery_analytics_dataset }}`.region_department ON enriched_offerer_data.offerer_department_code = region_department.num_dep
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
WHERE is_local_authority IS TRUE