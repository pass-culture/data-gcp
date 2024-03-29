{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_dbt_' ~ target_name) %}

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
        MIN(first_individual_booking_date) AS first_individual_booking_date,
        MAX(last_individual_booking_date) AS last_individual_booking_date,
        MIN(CASE WHEN offer_validation = 'APPROVED' THEN offer_creation_date END) AS first_individual_offer_creation_date,
        MAX(CASE WHEN offer_validation = 'APPROVED' THEN offer_creation_date END) AS last_individual_offer_creation_date,
        COUNT(CASE WHEN offer_validation = 'APPROVED' THEN offer_id END) AS total_created_individual_offers,
        COUNT(DISTINCT CASE WHEN is_bookable = 1 THEN offer_id END) AS total_venue_bookable_individual_offers,
        MIN(CASE WHEN is_bookable = 1 THEN offer_creation_date END) AS venue_first_bookable_individual_offer_date,
        MAX(CASE WHEN is_bookable = 1 THEN offer_creation_date END) AS venue_last_bookable_individual_offer_date,
    FROM {{ ref('int_applicative__offer') }}
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
        MIN(first_collective_booking_date) AS first_collective_booking_date,
        MAX(last_collective_booking_date) AS last_collective_booking_date,
        COUNT(CASE WHEN collective_offer_validation = 'APPROVED' THEN collective_offer_id END) AS total_created_collective_offers,
        MIN(CASE WHEN collective_offer_validation = 'APPROVED' THEN collective_offer_creation_date END) AS first_collective_offer_creation_date,
        MAX(CASE WHEN collective_offer_validation = 'APPROVED' THEN collective_offer_creation_date END) AS last_collective_offer_creation_date,
        COUNT(DISTINCT CASE WHEN is_bookable = 1 THEN collective_offer_id END) AS total_venue_bookable_collective_offers,
        MIN(CASE WHEN is_bookable = 1 THEN collective_offer_creation_date END) AS venue_first_bookable_collective_offer_date,
        MAX(CASE WHEN is_bookable = 1 THEN collective_offer_creation_date END) AS venue_last_bookable_collective_offer_date
    FROM {{ ref('int_applicative__collective_offer') }}
    GROUP BY venue_id
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
    v.venue_type_code,
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
    CONCAT(
        "https://passculture.pro/structures/",
        ofr.offerer_humanized_id,
        "/lieux/",
        {{target_schema}}.humanize_id(v.venue_id)
    ) AS venue_pc_pro_link,
    {{target_schema}}.humanize_id(v.venue_id) as venue_humanized_id,
    vr.venue_target AS venue_targeted_audience,
    vc.venue_contact_phone_number,
    vc.venue_contact_email,
    vc.venue_contact_website,
    vl.venue_label,
    ofr.offerer_id,
    ofr.offerer_name,
    ofr.offerer_validation_status,
    ofr.offerer_is_active,
    CASE WHEN v.venue_is_permanent THEN CONCAT("venue-",v.venue_id)
         ELSE CONCAT("offerer-", ofr.offerer_id) END AS partner_id,
    o.total_individual_bookings AS total_individual_bookings,
    co.total_collective_bookings AS total_collective_bookings,
    COALESCE(o.total_individual_bookings,0) + COALESCE(co.total_collective_bookings,0) AS total_bookings,
    COALESCE(o.total_non_cancelled_individual_bookings,0) AS total_non_cancelled_individual_bookings,
    COALESCE(co.total_non_cancelled_collective_bookings,0) AS total_non_cancelled_collective_bookings,
    o.first_individual_booking_date,
    o.last_individual_booking_date,
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
    COALESCE(o.total_individual_theoretic_revenue,0) + COALESCE(co.total_collective_theoretic_revenue,0) AS total_theoretic_revenue,
    COALESCE(o.total_individual_real_revenue,0) + COALESCE(co.total_collective_real_revenue,0) AS total_real_revenue,
    o.first_individual_offer_creation_date,
    o.last_individual_offer_creation_date,
    COALESCE(o.total_created_individual_offers,0) AS total_created_individual_offers,
    co.first_collective_offer_creation_date,
    co.last_collective_offer_creation_date,
    COALESCE(co.total_created_collective_offers,0) AS total_created_collective_offers,
    COALESCE(o.total_created_individual_offers,0) + COALESCE(co.total_created_collective_offers,0) AS total_created_offers,
    LEAST(
        o.venue_first_bookable_individual_offer_date,
        co.venue_first_bookable_collective_offer_date
    ) AS venue_first_bookable_offer_date,
    GREATEST(
        o.venue_last_bookable_individual_offer_date,
        co.venue_last_bookable_collective_offer_date
    ) AS venue_last_bookable_offer_date,
    CASE WHEN first_individual_booking_date IS NOT NULL AND first_collective_booking_date IS NOT NULL THEN LEAST(first_collective_booking_date, first_individual_booking_date)
         ELSE COALESCE(first_individual_booking_date,first_collective_booking_date) END AS first_booking_date,
    CASE WHEN last_individual_booking_date IS NOT NULL AND last_collective_booking_date IS NOT NULL THEN GREATEST(last_collective_booking_date, last_individual_booking_date)
         ELSE COALESCE(last_individual_booking_date,last_collective_booking_date) END AS last_booking_date,
    CASE WHEN first_individual_offer_creation_date IS NOT NULL AND first_collective_offer_creation_date IS NOT NULL THEN LEAST(first_collective_offer_creation_date, first_individual_offer_creation_date)
         ELSE COALESCE(first_individual_offer_creation_date,first_collective_offer_creation_date) END AS first_offer_creation_date,
    CASE WHEN last_individual_offer_creation_date IS NOT NULL AND last_collective_offer_creation_date IS NOT NULL THEN GREATEST(last_collective_offer_creation_date, last_individual_offer_creation_date)
        ELSE COALESCE(last_individual_offer_creation_date,last_collective_offer_creation_date) END AS last_offer_creation_date,
    COALESCE(o.total_venue_bookable_individual_offers,0) AS total_venue_bookable_individual_offers,
    COALESCE(co.total_venue_bookable_collective_offers,0) AS total_venue_bookable_collective_offers,
    COALESCE(o.total_venue_bookable_individual_offers,0) + COALESCE(co.total_venue_bookable_collective_offers,0) AS total_venue_bookable_offers
FROM {{ source("raw", "applicative_database_venue") }} AS v
LEFT JOIN offers_grouped_by_venue AS o ON o.venue_id = v.venue_id
LEFT JOIN collective_offers_grouped_by_venue AS co ON co.venue_id = v.venue_id
LEFT JOIN {{ source("raw", "applicative_database_venue_registration") }} AS vr ON v.venue_id = vr.venue_id
LEFT JOIN {{ source("raw", "applicative_database_venue_contact") }} AS vc ON v.venue_id = vc.venue_id
LEFT JOIN{{ source('raw', 'applicative_database_venue_label') }} AS vl ON vl.venue_label_id = v.venue_label_id
LEFT JOIN {{ ref("int_applicative__offerer") }} AS ofr ON v.venue_managing_offerer_id = ofr.offerer_id
