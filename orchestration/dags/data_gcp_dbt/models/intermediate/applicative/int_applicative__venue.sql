{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

WITH offers_grouped_by_venue AS (
    SELECT
        venue_id,
        SUM(total_individual_bookings) AS total_individual_bookings,
        SUM(total_non_cancelled_individual_bookings) AS total_non_cancelled_individual_bookings,
        SUM(total_used_individual_bookings) AS total_used_individual_bookings,
        SUM(total_individual_theoretic_revenue) AS total_individual_theoretic_revenue,
        SUM(total_individual_real_revenue) AS total_individual_real_revenue,
        SUM(total_individual_current_year_real_revenue) AS total_individual_current_year_real_revenue,
        MIN(first_individual_booking_date) AS first_individual_booking_date,
        MAX(last_individual_booking_date) AS last_individual_booking_date,
        MIN(first_stock_creation_date) AS first_stock_creation_date,
        MIN(CASE WHEN offer_validation = "APPROVED" THEN offer_creation_date END) AS first_individual_offer_creation_date,
        MAX(CASE WHEN offer_validation = "APPROVED" THEN offer_creation_date END) AS last_individual_offer_creation_date,
        COUNT(CASE WHEN offer_validation = "APPROVED" THEN offer_id END) AS total_created_individual_offers,
        COUNT(DISTINCT CASE WHEN offer_is_bookable THEN offer_id END) AS total_bookable_individual_offers,
        COUNT(DISTINCT venue_id) AS total_venues
    FROM {{ ref("int_applicative__offer") }}
    GROUP BY venue_id
),

collective_offers_grouped_by_venue AS (
    SELECT
        venue_id,
        SUM(total_collective_bookings) AS total_collective_bookings,
        SUM(total_non_cancelled_collective_bookings) AS total_non_cancelled_collective_bookings,
        SUM(total_used_collective_bookings) AS total_used_collective_bookings,
        SUM(total_collective_theoretic_revenue) AS total_collective_theoretic_revenue,
        SUM(total_collective_real_revenue) AS total_collective_real_revenue,
        SUM(total_collective_current_year_real_revenue) AS total_collective_current_year_real_revenue,
        MIN(first_collective_booking_date) AS first_collective_booking_date,
        MAX(last_collective_booking_date) AS last_collective_booking_date,
        COUNT(CASE WHEN collective_offer_validation = 'APPROVED' THEN collective_offer_id END) AS total_created_collective_offers,
        MIN(CASE WHEN collective_offer_validation = 'APPROVED' THEN collective_offer_creation_date END) AS first_collective_offer_creation_date,
        MAX(CASE WHEN collective_offer_validation = 'APPROVED' THEN collective_offer_creation_date END) AS last_collective_offer_creation_date,
        COUNT(DISTINCT CASE WHEN collective_offer_is_bookable THEN collective_offer_id END) AS total_bookable_collective_offers,
        SUM(total_non_cancelled_tickets) AS total_non_cancelled_tickets,
        SUM(total_current_year_non_cancelled_tickets) AS total_current_year_non_cancelled_tickets
    FROM {{ ref('int_applicative__collective_offer') }}
    GROUP BY venue_id
),

bookable_offer_history AS (
    SELECT
        venue_id,
        MIN(partition_date) AS first_bookable_offer_date,
        MAX(partition_date) AS last_bookable_offer_date,
        MIN(CASE WHEN individual_bookable_offers >0 THEN partition_date END) AS first_individual_bookable_offer_date,
        MAX(CASE WHEN individual_bookable_offers >0 THEN partition_date END) AS last_individual_bookable_offer_date,
        MIN(CASE WHEN collective_bookable_offers >0 THEN partition_date END) AS first_collective_bookable_offer_date,
        MAX(CASE WHEN collective_bookable_offers >0 THEN partition_date END) AS last_collective_bookable_offer_date
    FROM {{ ref('bookable_venue_history')}}
    GROUP BY venue_id
),

venues_with_geo_candidates AS (
    SELECT
        v.*,
        gi.iris_internal_id,
        gi.region_name,
        gi.iris_shape
    FROM {{ source("raw", "applicative_database_venue") }} AS v
    INNER JOIN {{ source('clean', 'geo_iris') }} AS gi
        ON v.venue_longitude BETWEEN gi.min_longitude AND gi.max_longitude
           AND v.venue_latitude BETWEEN gi.min_latitude AND gi.max_latitude
)

SELECT
    v.venue_thumb_count,
    v.venue_address,
    v.venue_postal_code,
    v.venue_city,
    v.ban_id,
    v.venue_id,
    v.venue_name,
    v.venue_siret,
    v.venue_latitude,
    v.venue_longitude,
    v.venue_managing_offerer_id,
    v.venue_booking_email,
    v.venue_is_virtual,
    v.venue_comment,
    v.venue_public_name,
    v.venue_type_code AS venue_type_label,
    v.venue_label_id,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.banner_url,
    v.venue_audioDisabilityCompliant,
    v.venue_mentalDisabilityCompliant,
    v.venue_motorDisabilityCompliant,
    v.venue_visualDisabilityCompliant,
    v.venue_adage_id,
    v.venue_educational_status_id,
    v.collective_description,
    v.collective_students,
    v.collective_website,
    v.collective_network,
    v.collective_intervention_area,
    v.collective_access_information,
    v.collective_phone,
    v.collective_email,
    v.dms_token,
    v.venue_description,
    v.venue_withdrawal_details,
    COALESCE(CASE WHEN v.venue_postal_code = "97150" THEN "978"
        WHEN SUBSTRING(v.venue_postal_code, 0, 2) = "97" THEN SUBSTRING(v.venue_postal_code, 0, 3)
        WHEN SUBSTRING(v.venue_postal_code, 0, 2) = "98" THEN SUBSTRING(v.venue_postal_code, 0, 3)
        WHEN SUBSTRING(v.venue_postal_code, 0, 3) in ("200", "201", "209", "205") THEN "2A"
        WHEN SUBSTRING(v.venue_postal_code, 0, 3) in ("202", "206") THEN "2B"
        ELSE SUBSTRING(v.venue_postal_code, 0, 2)
        END,
        v.venue_department_code
    ) AS venue_department_code,
    CONCAT(
        "https://backoffice.passculture.team/pro/venue/",
        v.venue_id
    ) AS venue_backoffice_link,
    {{target_schema}}.humanize_id(v.venue_id) as venue_humanized_id,
    venue_region_departement.academy_name AS venue_academy_name,
    vr.venue_target AS venue_targeted_audience,
    vc.venue_contact_phone_number,
    vc.venue_contact_email,
    vc.venue_contact_website,
    vl.venue_label,
    va.venue_id IS NOT NULL AS venue_is_acessibility_synched,
    o.total_individual_bookings AS total_individual_bookings,
    co.total_collective_bookings AS total_collective_bookings,
    COALESCE(o.total_individual_bookings,0) + COALESCE(co.total_collective_bookings,0) AS total_bookings,
    COALESCE(o.total_non_cancelled_individual_bookings,0) AS total_non_cancelled_individual_bookings,
    COALESCE(co.total_non_cancelled_collective_bookings,0) AS total_non_cancelled_collective_bookings,
    o.first_individual_booking_date,
    o.last_individual_booking_date,
    o.first_stock_creation_date,
    co.first_collective_booking_date,
    co.last_collective_booking_date,
    COALESCE(o.total_non_cancelled_individual_bookings,0) + COALESCE(co.total_non_cancelled_collective_bookings,0) AS total_non_cancelled_bookings,
    COALESCE(o.total_used_individual_bookings,0) + COALESCE(co.total_used_collective_bookings,0) AS total_used_bookings,
    COALESCE(o.total_used_individual_bookings,0) AS total_used_individual_bookings,
    COALESCE(co.total_used_collective_bookings,0) AS total_used_collective_bookings,
    COALESCE(o.total_individual_theoretic_revenue,0) AS total_individual_theoretic_revenue,
    COALESCE(o.total_individual_real_revenue,0) AS total_individual_real_revenue,
    COALESCE(co.total_collective_theoretic_revenue,0) AS total_collective_theoretic_revenue,
    COALESCE(co.total_collective_real_revenue,0) AS total_collective_real_revenue,
    COALESCE(o.total_individual_current_year_real_revenue,0) AS total_individual_current_year_real_revenue,
    COALESCE(co.total_collective_current_year_real_revenue,0) AS total_collective_current_year_real_revenue,
    COALESCE(o.total_individual_theoretic_revenue,0) + COALESCE(co.total_collective_theoretic_revenue,0) AS total_theoretic_revenue,
    COALESCE(o.total_individual_real_revenue,0) + COALESCE(co.total_collective_real_revenue,0) AS total_real_revenue,
    o.first_individual_offer_creation_date,
    o.last_individual_offer_creation_date,
    COALESCE(o.total_created_individual_offers,0) AS total_created_individual_offers,
    co.first_collective_offer_creation_date,
    co.last_collective_offer_creation_date,
    COALESCE(co.total_created_collective_offers,0) AS total_created_collective_offers,
    COALESCE(o.total_created_individual_offers,0) + COALESCE(co.total_created_collective_offers,0) AS total_created_offers,
    boh.first_bookable_offer_date,
    boh.last_bookable_offer_date,
    boh.first_individual_bookable_offer_date,
    boh.last_individual_bookable_offer_date,
    boh.first_collective_bookable_offer_date,
    boh.last_collective_bookable_offer_date,
    CASE WHEN o.first_individual_booking_date IS NOT NULL AND co.first_collective_booking_date IS NOT NULL THEN LEAST(co.first_collective_booking_date,o.first_individual_booking_date)
         ELSE COALESCE(first_individual_booking_date,first_collective_booking_date) END AS first_booking_date,
    CASE WHEN o.last_individual_booking_date IS NOT NULL AND co.last_collective_booking_date IS NOT NULL THEN GREATEST(co.last_collective_booking_date,o.last_individual_booking_date)
         ELSE COALESCE(o.last_individual_booking_date,co.last_collective_booking_date) END AS last_booking_date,
    CASE WHEN o.first_individual_offer_creation_date IS NOT NULL AND co.first_collective_offer_creation_date IS NOT NULL THEN LEAST(co.first_collective_offer_creation_date,o.first_individual_offer_creation_date)
         ELSE COALESCE(o.first_individual_offer_creation_date,co.first_collective_offer_creation_date) END AS first_offer_creation_date,
    CASE WHEN o.last_individual_offer_creation_date IS NOT NULL AND co.last_collective_offer_creation_date IS NOT NULL THEN GREATEST(co.last_collective_offer_creation_date,o.last_individual_offer_creation_date)
        ELSE COALESCE(o.last_individual_offer_creation_date,co.last_collective_offer_creation_date) END AS last_offer_creation_date,
    COALESCE(o.total_bookable_individual_offers,0) AS total_bookable_individual_offers,
    COALESCE(o.total_venues,0) AS total_venues,
    COALESCE(co.total_bookable_collective_offers,0) AS total_bookable_collective_offers,
    COALESCE(o.total_bookable_individual_offers,0) + COALESCE(co.total_bookable_collective_offers,0) AS total_bookable_offers,
    COALESCE(co.total_non_cancelled_tickets,0) AS total_non_cancelled_tickets,
    COALESCE(co.total_current_year_non_cancelled_tickets,0) AS total_current_year_non_cancelled_tickets,
    v.iris_internal_id AS venue_iris_internal_id,
    v.region_name AS venue_region_name,
FROM venues_with_geo_candidates AS v
LEFT JOIN offers_grouped_by_venue AS o ON o.venue_id = v.venue_id
LEFT JOIN collective_offers_grouped_by_venue AS co ON co.venue_id = v.venue_id
LEFT JOIN bookable_offer_history AS boh ON boh.venue_id = v.venue_id
LEFT JOIN {{ source("raw", "applicative_database_venue_registration") }} AS vr ON v.venue_id = vr.venue_id
LEFT JOIN {{ source("raw", "applicative_database_venue_contact") }} AS vc ON v.venue_id = vc.venue_id
LEFT JOIN {{ source("raw", "applicative_database_venue_label") }} AS vl ON vl.venue_label_id = v.venue_label_id
LEFT JOIN {{ source("raw", "applicative_database_accessibility_provider") }} AS va ON va.venue_id = v.venue_id
LEFT JOIN {{ source("analytics", "region_department") }} AS venue_region_departement ON v.venue_department_code = venue_region_departement.num_dep
WHERE ST_CONTAINS(
        v.iris_shape,
        ST_GEOGPOINT(v.venue_longitude, v.venue_latitude)
)
