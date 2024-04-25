WITH partner_activation AS (
SELECT 
  partner.partner_id,
  partner.venue_id,
  partner.offerer_id,
  partner.partner_creation_date,
  partner.partner_name,
  partner.partner_academy_name,
  partner.partner_department_code,
  partner.partner_region_name,
  partner.partner_status,
  partner.partner_type,
  partner.cultural_sector,
  offerer.legal_unit_business_activity_label,
  offerer.legal_unit_legal_category_label,
  CASE WHEN partner.individual_offers_created > 0 THEN TRUE ELSE FALSE END AS has_activated_individual_part,
  CASE WHEN partner.collective_offers_created > 0 THEN TRUE ELSE FALSE END AS has_activated_collective_part,
  CASE WHEN partner_status = "venue" THEN venue.first_individual_offer_creation_date
  ELSE offerer.offerer_first_individual_offer_creation_date END AS individual_activation_date,
  CASE WHEN partner_status = "venue" THEN venue.first_collective_offer_creation_date
  ELSE offerer.offerer_first_collective_offer_creation_date END AS collective_activation_date,
  partner.first_offer_creation_date as first_activation_date,
  partner.non_cancelled_individual_bookings as individual_bookings_after_first_activation,
  partner.confirmed_collective_bookings AS collective_bookings_after_first_activation
FROM {{ ref('enriched_cultural_partner_data') }} partner 
LEFT JOIN {{ ref('mrt_global__venue') }} venue on partner.venue_id = venue.venue_id
LEFT JOIN {{ ref('enriched_offerer_data') }} offerer on offerer.offerer_id = partner.offerer_id ) 

, partner_activation_stated AS (
SELECT 
  partner_activation.*,
  CASE 
    WHEN NOT has_activated_individual_part THEN "collective"
    WHEN NOT has_activated_collective_part THEN "individual"
    WHEN individual_activation_date < collective_activation_date THEN "individual" 
    ELSE "collective" 
  END AS first_activated_part,
  CASE 
    WHEN NOT has_activated_individual_part THEN NULL
    WHEN NOT has_activated_collective_part THEN NULL
    WHEN individual_activation_date < collective_activation_date THEN collective_activation_date
    ELSE individual_activation_date
  END AS second_activation_date,
  CASE 
    WHEN (NOT has_activated_individual_part OR NOT has_activated_collective_part) THEN NULL 
    WHEN individual_activation_date < collective_activation_date THEN DATE_DIFF(collective_activation_date, individual_activation_date, day) 
    ELSE DATE_DIFF(individual_activation_date, collective_activation_date, day) 
  END AS days_between_second_activation,
  CASE 
    WHEN NOT has_activated_individual_part THEN "collective only"
    WHEN NOT has_activated_collective_part THEN "individual only"
    ELSE "all part"
  END AS activation_state

FROM partner_activation 
WHERE (has_activated_individual_part OR has_activated_collective_part))

, indiv_bookings_after_activation AS (
SELECT 
  partner_activation_stated.partner_id, 
  COUNT(DISTINCT CASE WHEN booking_creation_date > second_activation_date THEN booking_id END) AS individual_bookings_after_second_activation
  
FROM partner_activation_stated
LEFT JOIN {{ ref('enriched_booking_data') }} on partner_activation_stated.partner_id = enriched_booking_data.partner_id AND NOT booking_is_cancelled
GROUP BY 1
)

, collec_bookings_after_activation AS (
SELECT 
  partner_activation_stated.partner_id, 
  COUNT(DISTINCT CASE WHEN collective_booking_creation_date > second_activation_date THEN collective_booking_id END) AS collective_bookings_after_second_activation,
  
FROM partner_activation_stated
LEFT JOIN {{ ref('enriched_collective_booking_data') }} on partner_activation_stated.partner_id = enriched_collective_booking_data.partner_id AND collective_booking_is_cancelled = "FALSE"
GROUP BY 1
)

, indiv_offers_after_activation AS (
SELECT 
  partner_activation_stated.partner_id, 
  COUNT(DISTINCT enriched_offer_data.offer_id) AS individual_offers_created_after_first_activation,
  COUNT(DISTINCT CASE WHEN offer_creation_date > second_activation_date THEN enriched_offer_data.offer_id END) AS individual_offers_created_after_second_activation
FROM partner_activation_stated
LEFT JOIN {{ ref('enriched_offer_data') }} on partner_activation_stated.partner_id = enriched_offer_data.partner_id
GROUP BY 1
)

, collec_offers_after_activation AS (
SELECT 
  partner_activation_stated.partner_id, 
  COUNT(DISTINCT enriched_collective_offer_data.collective_offer_id) AS collective_offers_created_after_first_activation,
  COUNT(DISTINCT CASE WHEN collective_offer_creation_date > second_activation_date THEN enriched_collective_offer_data.collective_offer_id END) AS collective_offers_created_after_second_activation
FROM partner_activation_stated
LEFT JOIN {{ ref('enriched_collective_offer_data') }} on partner_activation_stated.partner_id = enriched_collective_offer_data.partner_id
GROUP BY 1
)

SELECT 
  partner_activation_stated.*,
  indiv_bookings_after_activation.individual_bookings_after_second_activation,
  collec_bookings_after_activation.collective_bookings_after_second_activation,
  indiv_offers_after_activation.individual_offers_created_after_first_activation,
  indiv_offers_after_activation.individual_offers_created_after_second_activation,
  collec_offers_after_activation.collective_offers_created_after_first_activation,
  collec_offers_after_activation.collective_offers_created_after_second_activation,
  individual_bookings_after_first_activation - individual_bookings_after_second_activation AS individual_bookings_between_activations,
  collective_bookings_after_first_activation - collective_bookings_after_second_activation AS collective_bookings_between_activations,
  individual_offers_created_after_first_activation - individual_offers_created_after_second_activation AS individual_offers_created_between_activations,
  collective_offers_created_after_first_activation - collective_offers_created_after_second_activation AS collective_offers_created_between_activations,
FROM partner_activation_stated
LEFT JOIN indiv_bookings_after_activation on partner_activation_stated.partner_id = indiv_bookings_after_activation.partner_id
LEFT JOIN collec_bookings_after_activation on partner_activation_stated.partner_id = collec_bookings_after_activation.partner_id
LEFT JOIN indiv_offers_after_activation on partner_activation_stated.partner_id = indiv_offers_after_activation.partner_id
LEFT JOIN collec_offers_after_activation on partner_activation_stated.partner_id = collec_offers_after_activation.partner_id


