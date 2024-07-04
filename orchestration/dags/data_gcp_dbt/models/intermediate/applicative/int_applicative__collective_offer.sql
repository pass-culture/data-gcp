WITH collective_stocks_grouped_by_collective_offers AS (
    SELECT collective_offer_id,
    MAX(collective_stock_is_bookable) AS collective_stock_is_bookable, -- bookable = if at least one collective_stock is_bookable
    SUM(total_collective_bookings) AS total_collective_bookings,
    SUM(total_non_cancelled_collective_bookings) AS total_non_cancelled_collective_bookings,
    SUM(total_used_collective_bookings) AS total_used_collective_bookings,
    MIN(first_collective_booking_date) AS first_collective_booking_date,
    MAX(last_collective_booking_date) AS last_collective_booking_date,
    SUM(total_collective_theoretic_revenue) AS total_collective_theoretic_revenue,
    SUM(total_collective_real_revenue) AS total_collective_real_revenue,
    SUM(total_collective_current_year_real_revenue) AS total_collective_current_year_real_revenue,
    SUM(CASE WHEN total_non_cancelled_collective_bookings > 0 THEN collective_stock_number_of_tickets END) AS total_non_cancelled_tickets,
    SUM(CASE WHEN total_current_year_non_cancelled_collective_bookings > 0 THEN collective_stock_number_of_tickets END) AS total_current_year_non_cancelled_tickets
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
    co.venue_id,
    co.collective_offer_name,
    co.collective_offer_description,
    co.collective_offer_duration_minutes,
    DATE(collective_offer_creation_date) AS collective_offer_creation_date,
    collective_offer_creation_date AS collective_offer_created_at,
    co.collective_offer_subcategory_id,
    co.collective_offer_format,
    co.collective_offer_date_updated,
    co.collective_offer_students,
    NULL AS collective_offer_price_detail,
    co.collective_offer_booking_email,
    co.collective_offer_contact_email,
    co.collective_offer_contact_phone,
    NULL AS collective_offer_contact_url,
    NULL AS collective_offer_contact_form,
    co.collective_offer_offer_venue,
    co.collective_offer_venue_humanized_id,
    co.collective_offer_venue_address_type AS collective_offer_address_type,
    co.collective_offer_venue_other_address,
    co.intervention_area,
    co.template_id,
    co.collective_offer_last_validation_type,
    co.institution_id,
    co.collective_offer_image_id,
    co.provider_id,
    co.national_program_id,
    NULL AS collective_offer_template_beginning_date,
    NULL AS collective_offer_template_ending_date,
    CASE WHEN cs.collective_stock_is_bookable AND co.collective_offer_is_active THEN TRUE ELSE FALSE END AS collective_offer_is_bookable,
    cs.total_non_cancelled_collective_bookings,
    cs.total_collective_bookings,
    cs.total_used_collective_bookings,
    cs.total_collective_theoretic_revenue,
    cs.total_collective_real_revenue,
    cs.total_collective_current_year_real_revenue,
    cs.first_collective_booking_date,
    cs.last_collective_booking_date,
    cs.total_non_cancelled_tickets,
    cs.total_current_year_non_cancelled_tickets,
    il.institution_internal_iris_id,
    FALSE AS collective_offer_is_template,
FROM {{ source('raw','applicative_database_collective_offer') }} AS co
LEFT JOIN collective_stocks_grouped_by_collective_offers AS cs ON cs.collective_offer_id = co.collective_offer_id
LEFT JOIN {{ ref('int_applicative__educational_institution') }} AS ei ON ei.educational_institution_id = co.institution_id
LEFT JOIN {{ ref('institution_locations') }} AS il ON il.institution_id = ei.institution_id
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
    DATE(collective_offer_creation_date) AS collective_offer_creation_date,
    collective_offer_creation_date AS collective_offer_created_at,
    collective_offer_subcategory_id,
    collective_offer_format,
    collective_offer_date_updated,
    collective_offer_students,
    collective_offer_price_detail,
    collective_offer_booking_email,
    collective_offer_contact_email,
    collective_offer_contact_phone,
    collective_offer_contact_url,
    collective_offer_contact_form,
    collective_offer_offer_venue,
    collective_offer_venue_humanized_id,
    collective_offer_venue_address_type,
    collective_offer_venue_other_address,
    intervention_area,
    NULL AS template_id,
    collective_offer_last_validation_type,
    NULL AS institution_id,
    collective_offer_image_id,
    provider_id,
    national_program_id,
    collective_offer_template_beginning_date,
    collective_offer_template_ending_date,
    TRUE AS collective_offer_is_bookable,
    0 AS total_non_cancelled_collective_bookings,
    0 AS total_collective_bookings,
    0 AS total_used_collective_bookings,
    0 AS total_collective_theoretic_revenue,
    0 AS total_collective_real_revenue,
    0 AS total_collective_current_year_real_revenue,
    NULL AS first_collective_booking_date,
    NULL AS last_collective_booking_date,
    0 AS total_non_cancelled_tickets,
    0 AS total_current_year_non_cancelled_tickets,
    NULL AS institution_internal_iris_id,
    TRUE AS collective_offer_is_template,
FROM {{ source('raw','applicative_database_collective_offer_template') }}
)
