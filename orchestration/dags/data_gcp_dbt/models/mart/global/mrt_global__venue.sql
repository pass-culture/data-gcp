WITH venue_id_combination AS (

    SELECT DISTINCT venue_id
    FROM {{ ref('int_applicative__venue') }} v

),

venue_id_aggregated AS (
    SELECT
        vic.venue_id,
        SUM(o.total_individual_bookings) AS total_individual_bookings,
        SUM(co.total_collective_bookings) AS total_collective_bookings,
        SUM(o.total_non_cancelled_individual_bookings) AS total_non_cancelled_individual_bookings,
        SUM(o.total_used_individual_bookings) AS total_used_individual_bookings,
        SUM(o.total_individual_theoretic_revenue) AS total_individual_theoretic_revenue,
        SUM(o.total_individual_real_revenue) AS total_individual_real_revenue,
        MIN(o.first_individual_booking_date) AS first_individual_booking_date,
        MAX(o.last_individual_booking_date) AS last_individual_booking_date,
        MIN(CASE WHEN o.offer_validation = 'APPROVED' THEN o.offer_creation_date END) AS first_individual_offer_creation_date,
        MAX(CASE WHEN o.offer_validation = 'APPROVED' THEN o.offer_creation_date END) AS last_individual_offer_creation_date,
        COUNT(CASE WHEN o.offer_validation = 'APPROVED' THEN o.offer_id END) AS total_individual_offers_created,
        SUM(co.total_non_cancelled_collective_bookings) AS total_non_cancelled_collective_bookings,
        SUM(co.total_used_collective_bookings) AS total_used_collective_bookings,
        SUM(co.total_collective_theoretic_revenue) AS total_collective_theoretic_revenue,
        SUM(co.total_collective_real_revenue) AS total_collective_real_revenue,
        MIN(co.first_collective_booking_date) AS first_collective_booking_date,
        MAX(co.last_collective_booking_date) AS last_collective_booking_date,
        COUNT(CASE WHEN co.collective_offer_validation = 'APPROVED' THEN co.collective_offer_id END) AS total_collective_offers_created,
        MIN(CASE WHEN co.collective_offer_validation = 'APPROVED' THEN co.collective_offer_creation_date END) AS first_collective_offer_creation_date,
        MAX(CASE WHEN co.collective_offer_validation = 'APPROVED' THEN co.collective_offer_creation_date END) AS last_collective_offer_creation_date,
        COUNT(DISTINCT CASE WHEN o.is_bookable = 1 THEN o.offer_id END) AS total_venue_bookable_individual_offers,
        COUNT(DISTINCT CASE WHEN co.is_bookable = 1 THEN co.collective_offer_id END) AS total_venue_bookable_collective_offers,
        LEAST(
            MIN(CASE WHEN o.is_bookable = 1 THEN o.offer_creation_date END),
             MIN(CASE WHEN co.is_bookable = 1 THEN co.collective_offer_creation_date END)
        ) AS venue_first_bookable_offer_date,
        GREATEST(
            MAX(CASE WHEN o.is_bookable = 1 THEN o.offer_creation_date END),
             MAX(CASE WHEN co.is_bookable = 1 THEN co.collective_offer_creation_date END)
        ) AS venue_last_bookable_offer_date
    FROM venue_id_combination vic
        LEFT JOIN {{ ref('int_applicative__offer') }} AS o ON o.venue_id = vic.venue_id
        LEFT JOIN {{ ref('int_applicative__collective_offer') }} co ON co.venue_id = vic.venue_id
    GROUP BY
        venue_id
)

SELECT
    v.venue_id,
    CASE WHEN v.venue_is_permanent THEN CONCAT("venue-",v.venue_id)
         ELSE CONCAT("offerer-", v.offerer_id) END AS partner_id,
    v.venue_name,
    v.venue_public_name,
    v.venue_booking_email,
    v.venue_address,
    v.venue_latitude,
    v.venue_longitude,
    v.venue_department_code,
    v.venue_postal_code,
    v.venue_city,
    v.venue_siret,
    v.venue_is_virtual,
    v.venue_managing_offerer_id,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.offerer_name,
    v.offerer_validation_status,
    v.venue_type_code AS venue_type_label,
    v.venue_label,
    v.venue_humanized_id,
    v.venue_backoffice_link,
    venue_region_departement.region_name AS venue_region_name,
    v.venue_pc_pro_link,
    v.venue_targeted_audience,
    v.banner_url,
    v.venue_description,
    v.venue_withdrawal_details,
    v.venue_contact_phone_number,
    v.venue_contact_email,
    v.venue_contact_website,
    COALESCE(via.total_individual_bookings,0) + COALESCE(total_collective_bookings,0) AS total_bookings,
    COALESCE(via.total_non_cancelled_individual_bookings,0) AS total_non_cancelled_individual_bookings,
    COALESCE(via.total_non_cancelled_collective_bookings,0) AS total_non_cancelled_collective_bookings,
    via.first_individual_booking_date,
    via.last_individual_booking_date,
    via.first_collective_booking_date,
    via.last_collective_booking_date,
    COALESCE(via.total_non_cancelled_individual_bookings,0) + COALESCE(via.total_non_cancelled_collective_bookings,0) AS total_non_cancelled_bookings,
    COALESCE(via.total_used_individual_bookings,0) + COALESCE(via.total_used_collective_bookings,0) AS total_used_bookings,
    COALESCE(via.total_used_individual_bookings,0) AS total_used_individual_bookings,
    COALESCE(via.total_used_collective_bookings,0) AS total_used_collective_bookings,
    COALESCE(via.total_individual_theoretic_revenue,0) AS total_individual_theoretic_revenue,
    COALESCE(via.total_individual_real_revenue,0) AS total_individual_real_revenue,
    COALESCE(via.total_collective_theoretic_revenue,0) AS total_collective_theoretic_revenue,
    COALESCE(via.total_collective_real_revenue,0) AS total_collective_real_revenue,
    COALESCE(via.total_individual_theoretic_revenue,0) + COALESCE(via.total_collective_theoretic_revenue,0) AS total_theoretic_revenue,
    COALESCE(via.total_individual_real_revenue,0) + COALESCE(via.total_collective_real_revenue,0) AS total_real_revenue,
    via.first_individual_offer_creation_date,
    via.last_individual_offer_creation_date,
    COALESCE(via.total_individual_offers_created,0) AS total_individual_offers_created,
    via.first_collective_offer_creation_date,
    via.last_collective_offer_creation_date,
    COALESCE(via.total_collective_offers_created,0) AS total_collective_offers_created,
    COALESCE(via.total_individual_offers_created,0) + COALESCE(via.total_collective_offers_created,0) AS total_created_offers,
    via.venue_first_bookable_offer_date,
    via.venue_last_bookable_offer_date,
    LEAST(via.first_collective_booking_date, via.first_individual_booking_date) AS first_booking_date,
    GREATEST(via.last_collective_booking_date, via.last_individual_booking_date) AS last_booking_date,
    LEAST(via.first_collective_offer_creation_date, via.first_individual_offer_creation_date) AS first_offer_creation_date,
    GREATEST(last_collective_offer_creation_date, via.last_individual_offer_creation_date) AS last_offer_creation_date,
    COALESCE(via.total_venue_bookable_individual_offers,0) AS total_venue_bookable_individual_offers,
    COALESCE(via.total_venue_bookable_collective_offers,0) AS total_venue_bookable_collective_offers,
    COALESCE(via.total_venue_bookable_individual_offers,0) + COALESCE(via.total_venue_bookable_collective_offers,0) AS total_venue_bookable_offers
FROM {{ ref('int_applicative__venue') }} AS v
    LEFT JOIN venue_id_aggregated via ON via.venue_id = v.venue_id
    LEFT JOIN {{ source('analytics', 'region_department') }} AS venue_region_departement ON v.venue_department_code = venue_region_departement.num_dep
WHERE v.offerer_validation_status='VALIDATED'
    AND v.offerer_is_active
