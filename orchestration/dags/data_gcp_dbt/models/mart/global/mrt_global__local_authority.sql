WITH active_venues_last_30days AS (SELECT
    venue_managing_offerer_id AS offerer_id
    ,STRING_AGG(DISTINCT CONCAT(" ",CASE WHEN venue_type_label != "Offre numérique" THEN venue_type_label END)) AS active_last_30days_physical_venues_types
FROM {{ ref("mrt_global__venue") }} AS v
LEFT JOIN {{ ref("bookable_venue_history") }} ON v.venue_id = bookable_venue_history.venue_id
WHERE partition_date >= DATE("{{ ds() }}") - 30
GROUP BY offerer_id
)

SELECT DISTINCT
    ofr.offerer_id,
    REPLACE(ofr.partner_id,"offerer","local-authority") AS local_authority_id,
    ofr.offerer_name AS local_authority_name,
    CASE WHEN (LOWER(ofr.offerer_name) LIKE "commune%" OR LOWER(ofr.offerer_name) LIKE "%ville%de%") THEN "Communes"
    WHEN (LOWER(ofr.offerer_name) LIKE "%departement%" OR LOWER(ofr.offerer_name) LIKE "%département%") THEN "Départements"
    WHEN (LOWER(ofr.offerer_name) LIKE "region%" OR LOWER(ofr.offerer_name) LIKE "région%") THEN "Régions"
    WHEN (
        LOWER(ofr.offerer_name) LIKE "ca%" OR
        LOWER(ofr.offerer_name) LIKE "%agglo%" OR
        LOWER(ofr.offerer_name) LIKE "cc%" OR
        LOWER(ofr.offerer_name) LIKE "cu%" OR
        LOWER(ofr.offerer_name) LIKE "%communaute%" OR
        LOWER(ofr.offerer_name) LIKE "%agglomeration%" OR
        LOWER(ofr.offerer_name) LIKE "%agglomération%" OR
        LOWER(ofr.offerer_name) LIKE "%metropole%" OR
        LOWER(ofr.offerer_name) LIKE "%com%com%" OR
        LOWER(ofr.offerer_name) LIKE "%petr%" OR
        LOWER(ofr.offerer_name) LIKE "%intercommunal%"
    ) THEN "CC / Agglomérations / Métropoles"
    ELSE "Non qualifiable" END AS local_authority_type,
    CASE WHEN ofr.offerer_id IN (SELECT priority_offerer_id FROM {{ source("analytics","priority_local_authorities") }}) THEN TRUE ELSE FALSE END AS is_priority,
    COALESCE(ofr.offerer_validation_date,ofr.offerer_creation_date) AS local_authority_creation_date,
    CASE WHEN DATE_TRUNC(COALESCE(ofr.offerer_validation_date,ofr.offerer_creation_date),YEAR) <= DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE/*""{{ ds }}""*/),INTERVAL 1 YEAR),YEAR) THEN TRUE ELSE FALSE END AS was_registered_last_year,
    ofr.academy_name AS local_authority_academy_name,
    ofr.offerer_region_name AS local_authority_region_name,
    ofr.offerer_department_code AS local_authority_department_code,
    ofr.offerer_postal_code AS local_authority_postal_code,
    ofr.is_active_last_30days,
    ofr.is_active_current_year,
    ofr.total_managed_venues,
    ofr.total_physical_managed_venues,
    ofr.total_permanent_managed_venues,
    CASE WHEN ofr.total_administrative_venues >= 1 THEN TRUE ELSE FALSE END AS has_administrative_venue,
    ofr.all_physical_venues_types,
    active_last_30days_physical_venues_types,
    ofr.top_bookings_venue_type,
    ofr.top_real_revenue_venue_type,
    ofr.total_reimbursement_points,
    ofr.total_created_individual_offers,
    ofr.total_created_collective_offers,
    ofr.total_created_offers,
    ofr.first_offer_creation_date,
    ofr.last_bookable_offer_date,
    ofr.first_bookable_offer_date,
    ofr.total_non_cancelled_individual_bookings,
    ofr.total_used_individual_bookings,
    ofr.total_non_cancelled_collective_bookings,
    ofr.total_used_collective_bookings,
    ofr.total_individual_real_revenue,
    ofr.total_collective_real_revenue,
    ofr.total_real_revenue,
FROM {{ ref("int_global__offerer") }} AS ofr
LEFT JOIN active_venues_last_30days ON ofr.offerer_id = active_venues_last_30days.offerer_id
WHERE ofr.is_local_authority IS TRUE
    AND ofr.offerer_validation_status="VALIDATED"
    AND ofr.offerer_is_active
