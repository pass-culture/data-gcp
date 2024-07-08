WITH main_venue_tag_per_offerer AS (
SELECT
  venue_id,
  venue_managing_offerer_id,
  venue_tag_name AS partner_type,
  "venue_tag" AS partner_type_origin,
FROM {{ ref("mrt_global__venue_tag") }}
WHERE venue_tag_category_label = "Comptage partenaire sectoriel"
    AND offerer_rank_desc = 1
),

main_venue_type_per_offerer AS (
SELECT
    venue_id,
    venue_managing_offerer_id,
    venue_type_label AS partner_type,
    "venue_type_label" AS partner_type_origin,
FROM {{ ref("mrt_global__venue") }}
WHERE (total_created_offers > 0 OR venue_type_label != "Offre numérique")
    AND offerer_rank_asc = 1
),

top_venue_per_offerer AS (
SELECT
    main_venue_type_per_offerer.venue_managing_offerer_id,
    COALESCE(main_venue_tag_per_offerer.venue_id, main_venue_type_per_offerer.venue_id) AS venue_id,
    COALESCE(main_venue_tag_per_offerer.partner_type, main_venue_type_per_offerer.partner_type) AS partner_type,
    COALESCE(main_venue_tag_per_offerer.partner_type_origin, main_venue_type_per_offerer.partner_type_origin) AS partner_type_origin
FROM main_venue_type_per_offerer
LEFT JOIN main_venue_tag_per_offerer on main_venue_type_per_offerer.venue_managing_offerer_id = main_venue_tag_per_offerer.venue_managing_offerer_id
)

(
SELECT
    NULL AS venue_id,
    o.offerer_id,
    o.partner_id,
    o.offerer_creation_date AS partner_creation_date,
    CASE WHEN DATE_TRUNC(offerer_creation_date,YEAR) <= DATE_TRUNC(DATE_SUB(DATE("{{ ds() }}"),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE NULL END AS was_registered_last_year,
    o.offerer_name AS partner_name,
    o.academy_name AS partner_academy_name,
    o.offerer_region_name AS partner_region_name,
    o.offerer_department_code AS partner_department_code,
    o.offerer_postal_code AS partner_postal_code,
    COALESCE(o.partner_type,top_venue_per_offerer.partner_type, "Structure non tagguée") AS partner_type,
    CASE WHEN o.partner_type IS NOT NULL THEN "offerer_tag"
        WHEN top_venue_per_offerer.partner_type_origin = "venue_tag" THEN "most_active_venue_tag"
        WHEN top_venue_per_offerer.partner_type_origin= "venue_type_label" THEN "most_active_venue_type"
        ELSE NULL END AS partner_type_origin,
    agg_partner_cultural_sector.cultural_sector AS cultural_sector,
    o.dms_accepted_at AS dms_accepted_at,
    o.first_dms_adage_status AS first_dms_adage_status,
    o.is_reference_adage AS is_reference_adage,
    o.is_synchro_adage AS is_synchro_adage,
    o.is_active_last_30days,
    o.is_active_current_year,
    o.is_individual_active_last_30days,
    o.is_individual_active_current_year,
    o.is_collective_active_last_30days,
    o.is_collective_active_current_year,
    o.total_created_individual_offers AS individual_offers_created,
    o.total_created_collective_offers AS collective_offers_created,
    o.total_created_offers,
    o.first_offer_creation_date,
    o.first_individual_offer_creation_date,
    o.first_collective_offer_creation_date,
    o.last_bookable_offer_date,
    o.first_bookable_offer_date,
    o.first_individual_bookable_offer_date,
    o.last_individual_bookable_offer_date,
    o.first_collective_bookable_offer_date,
    o.last_collective_bookable_offer_date,
    o.total_non_cancelled_individual_bookings AS non_cancelled_individual_bookings,
    o.total_used_individual_bookings AS used_individual_bookings,
    o.total_non_cancelled_collective_bookings AS confirmed_collective_bookings,
    o.total_used_collective_bookings AS used_collective_bookings,
    o.total_individual_real_revenue AS real_individual_revenue,
    o.total_collective_real_revenue AS real_collective_revenue,
    o.total_real_revenue,
    "offerer" AS partner_status,
FROM {{ ref("mrt_global__offerer") }} AS o
LEFT JOIN {{ ref("mrt_global__venue") }} AS v
                                    ON v.offerer_id = o.offerer_id
                                    AND v.venue_is_permanent
LEFT JOIN top_venue_per_offerer ON top_venue_per_offerer.venue_managing_offerer_id = o.offerer_id
LEFT JOIN {{ source("raw", "agg_partner_cultural_sector") }} ON agg_partner_cultural_sector.partner_type = COALESCE(o.partner_type, top_venue_per_offerer.partner_type)
WHERE NOT o.is_local_authority
AND v.offerer_id IS NULL
)

UNION ALL

(
SELECT
    v.venue_id,
    v.venue_managing_offerer_id AS offerer_id,
    v.partner_id,
    venue_creation_date AS partner_creation_date,
    CASE WHEN DATE_TRUNC(venue_creation_date,YEAR) <= DATE_TRUNC(DATE_SUB(DATE("{{ ds() }}"),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE FALSE END AS was_registered_last_year,
    v.venue_name AS partner_name,
    v.venue_academy_name AS partner_academy_name,
    v.venue_region_name AS partner_region_name,
    v.venue_department_code AS partner_department_code,
    v.venue_postal_code AS partner_postal_code,
    COALESCE(vt.venue_tag_name, v.venue_type_label) AS partner_type,
    CASE WHEN vt.venue_tag_name IS NOT NULL THEN "venue_tag" ELSE "venue_type_label" END AS partner_type_origin,
    agg_partner_cultural_sector.cultural_sector AS cultural_sector,
    v.dms_accepted_at AS dms_accepted_at,
    v.first_dms_adage_status AS first_dms_adage_status,
    v.is_reference_adage AS is_reference_adage,
    v.is_synchro_adage AS is_synchro_adage,
    v.is_active_last_30days,
    v.is_active_current_year,
    v.is_individual_active_last_30days,
    v.is_individual_active_current_year,
    v.is_collective_active_last_30days,
    v.is_collective_active_current_year,
    v.total_created_individual_offers AS individual_offers_created,
    v.total_created_collective_offers AS collective_offers_created,
    v.total_created_offers AS total_offers_created,
    v.first_offer_creation_date AS first_offer_creation_date,
    v.first_individual_offer_creation_date AS first_individual_offer_creation_date,
    v.first_collective_offer_creation_date AS first_collective_offer_creation_date,
    v.last_bookable_offer_date,
    v.first_bookable_offer_date,
    v.first_individual_bookable_offer_date,
    v.last_individual_bookable_offer_date,
    v.first_collective_bookable_offer_date,
    v.last_collective_bookable_offer_date,
    v.total_non_cancelled_individual_bookings,
    v.total_used_individual_bookings,
    v.total_non_cancelled_collective_bookings,
    v.total_used_collective_bookings,
    v.total_individual_real_revenue,
    v.total_collective_real_revenue,
    v.total_real_revenue,
    "venue"AS partner_status,
FROM {{ ref("mrt_global__venue") }} AS v
LEFT JOIN {{ source("raw", "agg_partner_cultural_sector") }} ON agg_partner_cultural_sector.partner_type = v.venue_type_label
LEFT JOIN {{ ref("mrt_global__venue_tag") }} AS vt ON v.venue_id = vt.venue_id AND vt.venue_tag_category_label = "Comptage partenaire sectoriel"
WHERE venue_is_permanent
)
