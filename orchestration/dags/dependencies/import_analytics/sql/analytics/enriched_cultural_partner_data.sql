
{{ create_humanize_id_function() }}

WITH individual_bookings AS (
    SELECT 
        venue_id 
        ,offerer_id
        ,COUNT(*) AS non_cancelled_individual_bookings
        ,COUNT(CASE WHEN booking_is_used THEN 1 ELSE NULL END) AS used_individual_bookings
        ,SUM(CASE WHEN booking_is_used THEN booking_intermediary_amount ELSE NULL END) AS real_individual_CA
    FROM `{{ bigquery_analytics_dataset }}`.enriched_booking_data
    WHERE NOT booking_is_cancelled 
    GROUP BY 1,2 )
    
,bookable_individual_offer AS 

    (SELECT 
        venue_id 
        ,offerer_id
        ,MAX(partition_date) AS last_bookable_individual_offer
    FROM `{{ bigquery_analytics_dataset }}`.bookable_offer_history AS bookable_offer_history
    JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data AS enriched_offer_data
        ON bookable_offer_history.offer_id = enriched_offer_data.offer_id
    GROUP BY 1,2)
    
,individual_offers AS (
    SELECT 
        venue_id 
        ,offerer_id
        ,MIN(offer_creation_date) AS first_individual_offer_creation_date
        ,MAX(offer_creation_date) AS last_individual_offer_creation_date
        ,COUNT(*) AS individual_offers_created
    FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data
    GROUP BY 1,2 )
    
,collective_offers AS (
    SELECT 
        venue_id 
        ,offerer_id
        ,MIN(collective_offer_creation_date) AS first_collective_offer_creation_date
        ,MAX(collective_offer_creation_date) AS last_collective_offer_creation_date
        ,COUNT(*) AS collective_offers_created
    FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data
    GROUP BY 1,2 )

,bookable_collective_offer AS 

    (SELECT 
        venue_id 
        ,offerer_id
        ,MAX(partition_date) AS last_bookable_collective_offer
    FROM `{{ bigquery_analytics_dataset }}`.bookable_collective_offer_history AS bookable_collective_offer_history
    JOIN `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data AS enriched_collective_offer_data
        ON bookable_collective_offer_history.collective_offer_id = enriched_collective_offer_data.collective_offer_id
    GROUP BY 1,2)

    
,collective_bookings AS (
    SELECT 
        venue_id 
        ,offerer_id 
        ,COUNT(*) AS confirmed_collective_bookings 
        ,COUNT(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') THEN 1 ELSE NULL END) AS used_collective_bookings  
        ,SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') THEN booking_amount ELSE NULL END) AS real_collective_CA
    FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_booking_data
    WHERE collective_booking_status IN ('CONFIRMED','USED','REIMBURSED')
    GROUP BY 1,2)

,venues AS (SELECT 
    enriched_venue_data.venue_id AS venue_id 
    ,venue_managing_offerer_id AS offerer_id 
    ,venue_creation_date AS partner_creation_date 
    ,enriched_venue_data.venue_name AS name 
    ,region_department.academy_name AS partner_academy_name
    ,enriched_venue_data.venue_region_name AS partner_region_name
    ,enriched_venue_data.venue_department_code AS partner_department_code
    ,enriched_venue_data.venue_postal_code AS partner_postal_code
    ,'venue' AS partner_status 
    ,venue_type_label AS partner_type 
    ,venue_creation_date AS partner_creation_date 
    ,1 AS comptage
    ,collective_offers.collective_offers_created
    ,individual_offers.individual_offers_created
    ,(collective_offers.collective_offers_created+individual_offers.individual_offers_created) AS total_offers_created
    ,last_bookable_individual_offer
    ,last_bookable_collective_offer
    ,non_cancelled_individual_bookings
    ,used_individual_bookings
    ,confirmed_collective_bookings
    ,used_collective_bookings
    ,real_individual_CA
    ,real_collective_CA
    ,(real_collective_CA+real_individual_CA) AS total_real_CA
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data AS enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS region_department
    ON enriched_venue_data.venue_department_code = region_department.num_dep
LEFT JOIN individual_bookings ON enriched_venue_data.venue_id = individual_bookings.venue_id 
LEFT JOIN collective_bookings ON enriched_venue_data.venue_id = collective_bookings.venue_id 
LEFT JOIN bookable_individual_offer ON enriched_venue_data.venue_id = bookable_individual_offer.venue_id 
LEFT JOIN bookable_collective_offer ON enriched_venue_data.venue_id =bookable_collective_offer.venue_id 
LEFT JOIN individual_offers ON individual_offers.venue_id = enriched_venue_data.venue_id 
LEFT JOIN collective_offers ON collective_offers.venue_id = enriched_venue_data.venue_id 
AND venue_is_permanent IS TRUE)

,infos_tags  AS (
SELECT DISTINCT
    enriched_offer_data.offerer_id 
    ,STRING_AGG(DISTINCT CONCAT(' ',CASE WHEN offerer_tag_label IS NOT NULL THEN offerer_tag_label ELSE NULL END)) AS partner_type
    ,CASE WHEN festival_increments.festival_cnt IS NULL THEN 1 ELSE CAST(festival_cnt AS INT) END AS comptage
FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data AS enriched_offer_data
JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer_tag_mapping AS applicative_database_offerer_tag_mapping
    ON enriched_offer_data.offerer_id = applicative_database_offerer_tag_mapping.offerer_id
JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer_tag AS applicative_database_offerer_tag
    ON applicative_database_offerer_tag.offerer_tag_id = applicative_database_offerer_tag_mapping.tag_id
JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer_tag_category_mapping AS applicative_database_offerer_tag_category_mapping
    ON applicative_database_offerer_tag.offerer_tag_id = applicative_database_offerer_tag_category_mapping.offerer_tag_id
JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer_tag_category AS applicative_database_offerer_tag_category
    ON applicative_database_offerer_tag_category_mapping.offerer_tag_category_id = applicative_database_offerer_tag_category.offerer_tag_category_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.festival_increments AS festival_increments
    ON enriched_offer_data.offerer_id = festival_increments.offerer_id
GROUP BY 1,3)


SELECT DISTINCT
    '' AS venue_id
    ,enriched_offerer_data.offerer_id AS offerer_id 
    ,enriched_offerer_data.offerer_creation_date AS partner_creation_date
    ,enriched_offerer_data.offerer_name AS name 
    ,region_department.academy_name AS partner_academy_name
    ,enriched_offerer_data.offerer_region_name AS partner_region_name 
    ,enriched_offerer_data.offerer_department_code AS partner_department_code
    ,applicative_database_offerer.offerer_postal_code AS partner_postal_code
    ,'offerer' AS partner_status 
    ,partner_type
    ,comptage
    ,SUM(IFNULL(individual_offers_created,0)) AS individual_offers_created
    ,SUM(IFNULL(collective_offers_created,0)) AS collective_offers_created
    ,(SUM(IFNULL(individual_offers_created,0))+SUM(IFNULL(collective_offers_created,0))) AS total_offers_created
    ,MAX(last_bookable_individual_offer) AS last_bookable_individual_offer
    ,MAX(last_bookable_collective_offer) AS last_bookable_collective_offer
    ,SUM(IFNULL(non_cancelled_individual_bookings,0)) AS non_cancelled_individual_bookings
    ,SUM(IFNULL(used_individual_bookings,0)) AS used_individual_bookings
    ,SUM(IFNULL(confirmed_collective_bookings,0)) AS confirmed_collective_bookings
    ,SUM(IFNULL(used_collective_bookings,0)) AS used_collective_bookings
    ,SUM(IFNULL(real_individual_CA,0)) AS real_individual_CA
    ,SUM(IFNULL(real_collective_CA,0)) AS real_collective_CA
    ,(SUM(IFNULL(real_collective_CA,0))+SUM(IFNULL(real_individual_CA,0))) AS total_real_CA
FROM `{{ bigquery_analytics_dataset }}`.enriched_offerer_data AS enriched_offerer_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS applicative_database_offerer
    ON enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
LEFT JOIN infos_tags ON enriched_offerer_data.offerer_id = infos_tags.offerer_id 
LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS region_department
    ON enriched_offerer_data.offerer_department_code = region_department.num_dep
LEFT JOIN individual_bookings ON enriched_offerer_data.offerer_id = individual_bookings.offerer_id 
LEFT JOIN collective_bookings ON enriched_offerer_data.offerer_id = collective_bookings.offerer_id 
LEFT JOIN bookable_individual_offer ON enriched_offerer_data.offerer_id = bookable_individual_offer.offerer_id 
LEFT JOIN bookable_collective_offer ON enriched_offerer_data.offerer_id = bookable_collective_offer.offerer_id 
LEFT JOIN individual_offers ON enriched_offerer_data.offerer_id = individual_offers.offerer_id 
LEFT JOIN collective_offers ON enriched_offerer_data.offerer_id = collective_offers.offerer_id 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11