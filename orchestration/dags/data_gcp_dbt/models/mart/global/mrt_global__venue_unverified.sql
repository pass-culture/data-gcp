SELECT
    v.venue_id,
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
    v.venue_is_acessibility_synched,
    v.venue_type_label,
    v.venue_label,
    v.venue_humanized_id,
    v.venue_backoffice_link,
    v.venue_region_name,
    v.venue_academy_name,
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
    v.first_bookable_offer_date,
    v.last_bookable_offer_date,
    v.first_individual_bookable_offer_date,
    v.last_individual_bookable_offer_date,
    v.first_collective_bookable_offer_date,
    v.last_collective_bookable_offer_date,
    v.first_booking_date,
    v.last_booking_date,
    v.first_offer_creation_date,
    v.last_offer_creation_date,
    v.total_bookable_individual_offers,
    v.total_bookable_collective_offers,
    v.total_bookable_offers,
    v.venue_iris_internal_id,
    v.total_non_cancelled_tickets,
    v.total_current_year_non_cancelled_tickets,
    ofr.offerer_id,
    ofr.offerer_name,
    ofr.offerer_validation_status,
    ofr.offerer_is_active,
    CONCAT(
        "https://passculture.pro/structures/",
        ofr.offerer_humanized_id,
        "/lieux/",
        venue_humanized_id
    ) AS venue_pc_pro_link,
    CASE WHEN v.venue_is_permanent THEN CONCAT("venue-",v.venue_id)
         ELSE ofr.partner_id END AS partner_id
FROM {{ ref('int_applicative__venue') }} AS v
LEFT JOIN {{ ref('mrt_global__offerer') }} AS ofr ON v.venue_managing_offerer_id = ofr.offerer_id
