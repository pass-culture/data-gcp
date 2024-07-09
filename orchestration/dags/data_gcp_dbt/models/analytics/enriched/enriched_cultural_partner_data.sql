WITH permanent_venues AS
(
SELECT
    mrt_global__venue.venue_id AS venue_id
    ,mrt_global__venue.venue_managing_offerer_id AS offerer_id
    ,mrt_global__venue.partner_id
    ,venue_creation_date AS partner_creation_date
    ,CASE WHEN DATE_TRUNC(venue_creation_date,YEAR) <= DATE_TRUNC(DATE_SUB(DATE('{{ ds() }}'),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE FALSE END AS was_registered_last_year
    ,mrt_global__venue.venue_name AS partner_name
    ,region_department.academy_name AS partner_academy_name
    ,mrt_global__venue.venue_region_name AS partner_region_name
    ,mrt_global__venue.venue_department_code AS partner_department_code
    ,mrt_global__venue.venue_postal_code AS partner_postal_code
    ,'venue' AS partner_status
    ,COALESCE(venue_tag_name, venue_type_label) AS partner_type
    ,CASE WHEN 
        venue_tag_name IS NOT NULL THEN "venue_tag"
        ELSE 'venue_type_label' 
        END
    AS partner_type_origin
    ,agg_partner_cultural_sector.cultural_sector AS cultural_sector
    ,enriched_offerer_data.dms_accepted_at AS dms_accepted_at
    ,enriched_offerer_data.first_dms_adage_status AS first_dms_adage_status
    ,enriched_offerer_data.is_reference_adage AS is_reference_adage
    ,enriched_offerer_data.is_synchro_adage AS is_synchro_adage
    ,CASE WHEN DATE_DIFF(CURRENT_DATE, last_bookable_offer_date, DAY) <= 30 THEN TRUE ELSE FALSE END AS is_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE, last_bookable_offer_date, YEAR) = 0 THEN TRUE ELSE FALSE END AS is_active_current_year
    ,CASE WHEN DATE_DIFF(CURRENT_DATE, last_individual_bookable_offer_date, DAY) <= 30 THEN TRUE ELSE FALSE END AS is_individual_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE, last_individual_bookable_offer_date, YEAR) = 0 THEN TRUE ELSE FALSE END AS is_individual_active_current_year
    ,CASE WHEN DATE_DIFF(CURRENT_DATE, last_collective_bookable_offer_date, DAY) <= 30 THEN TRUE ELSE FALSE END AS is_collective_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE, last_collective_bookable_offer_date, YEAR) = 0 THEN TRUE ELSE FALSE END AS is_collective_active_current_year
    ,COALESCE(mrt_global__venue.total_created_individual_offers,0) AS individual_offers_created
    ,COALESCE(mrt_global__venue.total_created_collective_offers,0) AS collective_offers_created
    ,(COALESCE(mrt_global__venue.total_created_collective_offers,0) + COALESCE(mrt_global__venue.total_created_individual_offers,0)) AS total_offers_created
    ,mrt_global__venue.first_offer_creation_date AS first_offer_creation_date
    ,mrt_global__venue.first_individual_offer_creation_date AS first_individual_offer_creation_date
    ,mrt_global__venue.first_collective_offer_creation_date AS first_collective_offer_creation_date
    ,last_bookable_offer_date
    ,first_bookable_offer_date 
    ,first_individual_bookable_offer_date
    ,last_individual_bookable_offer_date
    ,first_collective_bookable_offer_date
    ,last_collective_bookable_offer_date
    ,COALESCE(mrt_global__venue.total_non_cancelled_individual_bookings,0) AS non_cancelled_individual_bookings
    ,COALESCE(mrt_global__venue.total_used_individual_bookings,0) AS used_individual_bookings
    ,COALESCE(mrt_global__venue.total_non_cancelled_collective_bookings,0) AS confirmed_collective_bookings
    ,COALESCE(mrt_global__venue.total_used_collective_bookings,0) AS used_collective_bookings
    ,COALESCE(mrt_global__venue.total_individual_real_revenue,0) AS real_individual_revenue
    ,COALESCE(mrt_global__venue.total_collective_real_revenue,0) AS real_collective_revenue
    ,(COALESCE(mrt_global__venue.total_individual_real_revenue,0)+COALESCE(mrt_global__venue.total_collective_real_revenue,0)) AS total_real_revenue
FROM {{ ref('mrt_global__venue') }} AS mrt_global__venue
LEFT JOIN {{ source('analytics', 'region_department') }} AS region_department
    ON mrt_global__venue.venue_department_code = region_department.num_dep
LEFT JOIN {{ source('raw', 'agg_partner_cultural_sector') }} ON agg_partner_cultural_sector.partner_type = mrt_global__venue.venue_type_label
LEFT JOIN {{ ref('mrt_global__venue_tag') }} AS mrt_global__venue_tag ON mrt_global__venue.venue_id = mrt_global__venue_tag.venue_id AND mrt_global__venue_tag.venue_tag_category_label = "Comptage partenaire sectoriel"
LEFT JOIN {{ ref('enriched_offerer_data') }} AS enriched_offerer_data
    ON mrt_global__venue.venue_managing_offerer_id = enriched_offerer_data.offerer_id
WHERE venue_is_permanent IS TRUE
),


tagged_partners AS (
SELECT
    offerer_id
    ,STRING_AGG(DISTINCT (CASE WHEN tag_label IS NOT NULL THEN tag_label ELSE NULL END) ORDER BY (CASE WHEN tag_label IS NOT NULL THEN tag_label ELSE NULL END)) AS partner_type
FROM {{ ref('enriched_offerer_tags_data') }}
WHERE tag_category_name = 'comptage'
AND tag_label NOT IN ('Association', 'EPN','Collectivité','Pas de tag associé','Auto-Entrepreneur','Compagnie','Tourneur')
GROUP BY 1
),

-- On récupère tous les lieux taggués et on remonte le tag du lieu le + actif de chaque structure
top_venue_tag_per_offerer AS (
SELECT 
  mrt_global__venue.venue_id,
  mrt_global__venue.venue_managing_offerer_id AS offerer_id,
  venue_tag_name AS partner_type,
  'venue_tag' AS partner_type_origin
FROM {{ ref('mrt_global__venue') }} AS mrt_global__venue
JOIN {{ ref('mrt_global__venue_tag') }} AS mrt_global__venue_tag ON mrt_global__venue.venue_id = mrt_global__venue_tag.venue_id
AND mrt_global__venue_tag.venue_tag_category_label = "Comptage partenaire sectoriel"
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY mrt_global__venue.venue_managing_offerer_id
    ORDER BY 
        total_theoretic_revenue DESC
        , (COALESCE(mrt_global__venue.total_created_individual_offers,0) + COALESCE(mrt_global__venue.total_created_collective_offers,0)) DESC
        , venue_name
) = 1
),

-- On récupère le label du lieu le + actif de chaque structure
top_venue_type_per_offerer AS (
SELECT
    mrt_global__venue.venue_id
    ,venue_managing_offerer_id AS offerer_id
    ,venue_type_label AS partner_type
    ,'venue_type_label'
    AS partner_type_origin
FROM {{ ref('mrt_global__venue') }} AS mrt_global__venue
WHERE (total_created_offers > 0 OR venue_type_label != 'Offre numérique')
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY venue_managing_offerer_id 
    ORDER BY 
        total_theoretic_revenue DESC
        , (COALESCE(mrt_global__venue.total_created_individual_offers,0) + COALESCE(mrt_global__venue.total_created_collective_offers,0)) DESC
        , venue_name ASC
) = 1
),

top_venue_per_offerer AS (
SELECT 
    top_venue_type_per_offerer.offerer_id,
    COALESCE(top_venue_tag_per_offerer.venue_id, top_venue_type_per_offerer.venue_id) venue_id,
    COALESCE(top_venue_tag_per_offerer.partner_type, top_venue_type_per_offerer.partner_type) partner_type,
    COALESCE(top_venue_tag_per_offerer.partner_type_origin, top_venue_type_per_offerer.partner_type_origin) partner_type_origin
FROM top_venue_type_per_offerer 
LEFT JOIN top_venue_tag_per_offerer on top_venue_type_per_offerer.offerer_id = top_venue_tag_per_offerer.offerer_id 
),


offerers AS (
SELECT
    '' AS venue_id
    ,enriched_offerer_data.offerer_id
    ,enriched_offerer_data.partner_id
    ,enriched_offerer_data.offerer_creation_date AS partner_creation_date
    ,CASE WHEN DATE_TRUNC(enriched_offerer_data.offerer_creation_date,YEAR) <= DATE_TRUNC(DATE_SUB(DATE('{{ ds() }}'),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE NULL END AS was_registered_last_year
    ,enriched_offerer_data.offerer_name AS partner_name
    ,region_department.academy_name AS partner_academy_name
    ,enriched_offerer_data.offerer_region_name AS partner_region_name
    ,enriched_offerer_data.offerer_department_code AS partner_department_code
    ,applicative_database_offerer.offerer_postal_code AS partner_postal_code
    ,'offerer' AS partner_status
    ,COALESCE(tagged_partners.partner_type,top_venue_per_offerer.partner_type, 'Structure non tagguée') AS partner_type
    ,CASE
        WHEN tagged_partners.partner_type IS NOT NULL THEN 'offerer_tag'
        WHEN top_venue_per_offerer.partner_type_origin = "venue_tag" THEN 'most_active_venue_tag'
        WHEN top_venue_per_offerer.partner_type_origin= "venue_type_label" THEN "most_active_venue_type"
        ELSE NULL END AS partner_type_origin
    ,agg_partner_cultural_sector.cultural_sector AS cultural_sector
    ,enriched_offerer_data.dms_accepted_at AS dms_accepted_at
    ,enriched_offerer_data.first_dms_adage_status AS first_dms_adage_status
    ,enriched_offerer_data.is_reference_adage AS is_reference_adage
    ,enriched_offerer_data.is_synchro_adage AS is_synchro_adage
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,enriched_offerer_data.offerer_last_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,enriched_offerer_data.offerer_last_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_active_current_year
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,offerer_last_individual_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_individual_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,offerer_last_individual_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_individual_active_current_year
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,offerer_last_collective_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_collective_active_last_30days
    ,CASE WHEN DATE_DIFF(CURRENT_DATE,offerer_last_collective_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_collective_active_current_year
    ,COALESCE(enriched_offerer_data.offerer_individual_offers_created,0) AS individual_offers_created
    ,COALESCE(enriched_offerer_data.offerer_collective_offers_created,0) AS collective_offers_created
    ,COALESCE(enriched_offerer_data.offerer_individual_offers_created,0) + COALESCE(enriched_offerer_data.offerer_collective_offers_created,0) AS total_offers_created
    ,enriched_offerer_data.offerer_first_offer_creation_date AS first_offer_creation_date
    ,enriched_offerer_data.offerer_first_individual_offer_creation_date AS first_individual_offer_creation_date
    ,enriched_offerer_data.offerer_first_collective_offer_creation_date AS first_collective_offer_creation_date
    ,enriched_offerer_data.offerer_last_bookable_offer_date AS last_bookable_offer_date
    ,enriched_offerer_data.offerer_first_bookable_offer_date AS first_bookable_offer_date
    ,enriched_offerer_data.offerer_first_individual_bookable_offer_date AS first_individual_bookable_offer_date
    ,enriched_offerer_data.offerer_last_individual_bookable_offer_date AS last_individual_bookable_offer_date
    ,enriched_offerer_data.offerer_first_collective_bookable_offer_date AS first_collective_bookable_offer_date
    ,enriched_offerer_data.offerer_last_collective_bookable_offer_date AS last_collective_bookable_offer_date
    ,COALESCE(enriched_offerer_data.offerer_non_cancelled_individual_bookings,0) AS non_cancelled_individual_bookings
    ,COALESCE(enriched_offerer_data.offerer_used_individual_bookings,0) AS used_individual_bookings
    ,COALESCE(enriched_offerer_data.offerer_non_cancelled_collective_bookings,0) AS confirmed_collective_bookings
    ,COALESCE(enriched_offerer_data.offerer_used_collective_bookings,0) AS used_collective_bookings
    ,COALESCE(enriched_offerer_data.offerer_individual_real_revenue,0) AS real_individual_revenue
    ,COALESCE(enriched_offerer_data.offerer_collective_real_revenue,0) AS real_collective_revenue
    ,COALESCE(enriched_offerer_data.offerer_individual_real_revenue,0) + COALESCE(enriched_offerer_data.offerer_collective_real_revenue,0) AS total_real_revenue
FROM {{ ref('enriched_offerer_data') }}
LEFT JOIN {{ source('raw', 'applicative_database_offerer') }} AS applicative_database_offerer
    ON enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
LEFT JOIN {{ source('analytics', 'region_department') }} AS region_department
    ON enriched_offerer_data.offerer_department_code = region_department.num_dep
LEFT JOIN tagged_partners ON tagged_partners.offerer_id = enriched_offerer_data.offerer_id
LEFT JOIN permanent_venues ON permanent_venues.offerer_id = enriched_offerer_data.offerer_id
LEFT JOIN top_venue_per_offerer ON top_venue_per_offerer.offerer_id = enriched_offerer_data.offerer_id
LEFT JOIN {{ source('raw', 'agg_partner_cultural_sector') }} ON agg_partner_cultural_sector.partner_type = COALESCE(tagged_partners.partner_type, top_venue_per_offerer.partner_type)
WHERE NOT enriched_offerer_data.is_local_authority  -- Collectivités à part
AND permanent_venues.offerer_id IS NULL -- Pas déjà compté à l'échelle du lieu permanent
)

SELECT
    venue_id
    ,offerer_id
    ,partner_id
    ,partner_creation_date
    ,was_registered_last_year
    ,partner_name
    ,partner_academy_name
    ,partner_region_name
    ,partner_department_code
    ,partner_postal_code
    ,partner_status
    ,partner_type
    ,partner_type_origin
    ,cultural_sector
    ,is_active_last_30days
    ,is_active_current_year
    ,is_individual_active_last_30days
    ,is_individual_active_current_year
    ,is_collective_active_last_30days
    ,is_collective_active_current_year
    ,individual_offers_created
    ,collective_offers_created
    ,total_offers_created
    ,first_offer_creation_date
    ,first_individual_offer_creation_date
    ,first_collective_offer_creation_date
    ,last_bookable_offer_date
    ,first_bookable_offer_date
    ,first_individual_bookable_offer_date
    ,last_individual_bookable_offer_date
    ,first_collective_bookable_offer_date
    ,last_collective_bookable_offer_date
    ,non_cancelled_individual_bookings
    ,used_individual_bookings
    ,confirmed_collective_bookings
    ,used_collective_bookings
    ,real_individual_revenue
    ,real_collective_revenue
    ,total_real_revenue
FROM permanent_venues
UNION ALL
SELECT
    venue_id
    ,offerer_id
    ,partner_id
    ,partner_creation_date
    ,was_registered_last_year
    ,partner_name
    ,partner_academy_name
    ,partner_region_name
    ,partner_department_code
    ,partner_postal_code
    ,partner_status
    ,partner_type
    ,partner_type_origin
    ,cultural_sector
    ,is_active_last_30days
    ,is_active_current_year
    ,is_individual_active_last_30days
    ,is_individual_active_current_year
    ,is_collective_active_last_30days
    ,is_collective_active_current_year
    ,individual_offers_created
    ,collective_offers_created
    ,total_offers_created
    ,first_offer_creation_date
    ,first_individual_offer_creation_date
    ,first_collective_offer_creation_date
    ,last_bookable_offer_date
    ,first_bookable_offer_date
    ,first_individual_bookable_offer_date
    ,last_individual_bookable_offer_date
    ,first_collective_bookable_offer_date
    ,last_collective_bookable_offer_date
    ,non_cancelled_individual_bookings
    ,used_individual_bookings
    ,confirmed_collective_bookings
    ,used_collective_bookings
    ,real_individual_revenue
    ,real_collective_revenue
    ,total_real_revenue
FROM offerers
