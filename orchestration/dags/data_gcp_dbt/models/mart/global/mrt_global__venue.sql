SELECT
    v.venue_id,
    v.partner_id,
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
    v.venue_is_acessibility_synched,
    v.venue_type_label,
    v.venue_label,
    v.venue_humanized_id,
    v.venue_backoffice_link,
    v.venue_region_name,
    v.venue_academy_name,
    v.venue_pc_pro_link,
    v.venue_targeted_audience,
    v.banner_url,
    v.venue_description,
    v.venue_withdrawal_details,
    v.venue_contact_phone_number,
    v.venue_contact_email,
    v.venue_contact_website,
    v.total_individual_bookings,
    v.total_collective_bookings,
    v.total_bookings,
    v.total_non_cancelled_individual_bookings,
    v.total_non_cancelled_collective_bookings,
    v.first_individual_booking_date,
    v.last_individual_booking_date,
    v.first_collective_booking_date,
    v.last_collective_booking_date,
    v.total_non_cancelled_bookings,
    v.total_used_bookings,
    v.total_used_individual_bookings,
    v.total_used_collective_bookings,
    v.total_individual_theoretic_revenue,
    v.total_individual_real_revenue,
    v.total_collective_theoretic_revenue,
    v.total_collective_real_revenue,
    v.total_theoretic_revenue,
    v.total_real_revenue,
    v.first_individual_offer_creation_date,
    v.last_individual_offer_creation_date,
    v.total_created_individual_offers,
    v.first_collective_offer_creation_date,
    v.last_collective_offer_creation_date,
    v.total_created_collective_offers,
    v.total_created_offers,
    v.venue_first_bookable_offer_date,
    v.venue_last_bookable_offer_date,
    v.first_booking_date,
    v.last_booking_date,
    v.first_offer_creation_date,
    v.last_offer_creation_date,
    v.total_venue_bookable_individual_offers,
    v.total_venue_bookable_collective_offers,
    v.total_venue_bookable_offers,
    v.cicd_test_field_name as cicd_test_field_name_child
FROM {{ ref('int_applicative__venue') }} AS v
LEFT JOIN {{ source('analytics', 'region_department') }} AS venue_region_departement ON v.venue_department_code = venue_region_departement.num_dep
WHERE v.offerer_validation_status='VALIDATED'
    AND v.offerer_is_active
