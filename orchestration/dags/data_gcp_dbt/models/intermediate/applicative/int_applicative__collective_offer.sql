WITH collective_stocks_grouped_by_collective_offers AS (
    SELECT collective_offer_id,
    MAX(is_bookable) AS is_bookable, -- bookable = if at least one collective_offer has is_bookable = 1
    SUM(total_collective_bookings) AS total_collective_bookings,
    SUM(total_non_cancelled_collective_bookings) AS total_non_cancelled_collective_bookings,
    SUM(total_used_collective_bookings) AS total_used_collective_bookings,
    MIN(first_collective_booking_date) AS first_collective_booking_date,
    MAX(last_collective_booking_date) AS last_collective_booking_date,
    SUM(total_collective_theoretic_revenue) AS total_collective_theoretic_revenue,
    SUM(total_collective_real_revenue) AS total_collective_real_revenue
    FROM {{ ref('int_applicative__collective_stock') }}
    GROUP BY collective_offer_id
)

(
SELECT
    co.collective_offer_audio_disability_compliant,
    co.collective_offer_mental_disability_compliant,
    co.collective_offer_motor_disability_compliant,
    co.collective_offer_visual_disability_compliant,
    co.collective_offer_last_validation_date,
    co.collective_offer_validation,
    co.collective_offer_id,
    co.offer_id,
    co.collective_offer_is_active,
    COALESCE(co.collective_offer_venue_humanized_id,co.venue_id) AS venue_id,
    co.collective_offer_name,
    co.collective_offer_description,
    co.collective_offer_duration_minutes,
    co.collective_offer_creation_date,
    co.collective_offer_subcategory_id,
    co.collective_offer_format,
    co.collective_offer_date_updated,
    co.collective_offer_students,
    null as collective_offer_price_detail,
    co.collective_offer_booking_email,
    co.collective_offer_contact_email,
    co.collective_offer_contact_phone,
    co.collective_offer_offer_venue,
    co.collective_offer_venue_humanized_id,
    co.collective_offer_venue_address_type,
    co.collective_offer_venue_other_address,
    co.intervention_area,
    co.template_id,
    co.collective_offer_last_validation_type,
    co.institution_id,
    co.collective_offer_image_id,
    co.provider_id,
    co.national_program_id,
    null as collective_offer_template_beginning_date,
    null as collective_offer_template_ending_date,
    CASE WHEN cs.is_bookable = 1 AND co.collective_offer_is_active THEN 1 ELSE 0 END AS is_bookable,
    cs.total_non_cancelled_collective_bookings,
    cs.total_collective_bookings,
    cs.total_used_collective_bookings,
    cs.total_collective_theoretic_revenue,
    cs.total_collective_real_revenue,
    cs.first_collective_booking_date,
    cs.last_collective_booking_date,
    0 AS is_template
FROM {{ source('raw','applicative_database_collective_offer') }} AS co
LEFT JOIN collective_stocks_grouped_by_collective_offers AS cs ON cs.collective_offer_id = co.collective_offer_id
)
UNION ALL
(
SELECT
    collective_offer_audio_disability_compliant,
    collective_offer_mental_disability_compliant,
    collective_offer_motor_disability_compliant,
    collective_offer_visual_disability_compliant,
    collective_offer_last_validation_date,
    collective_offer_validation,
    collective_offer_id,
    offer_id,
    collective_offer_is_active,
    venue_id,
    collective_offer_name,
    collective_offer_description,
    collective_offer_duration_minutes,
    collective_offer_creation_date,
    collective_offer_subcategory_id,
    collective_offer_format,
    collective_offer_date_updated,
    collective_offer_students,
    collective_offer_price_detail,
    collective_offer_booking_email,
    collective_offer_contact_email,
    collective_offer_contact_phone,
    collective_offer_offer_venue,
    collective_offer_venue_humanized_id,
    collective_offer_venue_address_type,
    collective_offer_venue_other_address,
    intervention_area,
    null as template_id,
    collective_offer_last_validation_type,
    null as institution_id,
    collective_offer_image_id,
    provider_id,
    national_program_id,
    collective_offer_template_beginning_date,
    collective_offer_template_ending_date,
    1 AS is_bookable,
    0 as total_non_cancelled_collective_bookings,
    0 as total_collective_bookings,
    0 as total_used_collective_bookings,
    0 as total_collective_theoretic_revenue,
    0 as total_collective_real_revenue,
    null as first_collective_booking_date,
    null as last_collective_booking_date,
    1 AS is_template
FROM {{ source('raw','applicative_database_collective_offer_template') }} AS template
)
