WITH individual_data AS (
SELECT
    offerer_siren
    ,CASE WHEN offer.offer_id IS NULL THEN NULL ELSE "INDIVIDUAL" END AS individual_collective
    ,venue.venue_id
    ,venue.venue_name
    ,venue.venue_public_name
    ,subcategories.category_id
    ,subcategories.id AS subcategory
    ,offer.offer_id
    ,offer.offer_name
    ,COUNT(DISTINCT CASE WHEN booking.booking_status IN ('USED', 'REIMBURSED') THEN booking.booking_id ELSE NULL END) AS count_bookings -- will be deleted
    ,COUNT(DISTINCT CASE WHEN booking.booking_status IN ('USED', 'REIMBURSED') THEN booking.booking_id ELSE NULL END) AS count_used_bookings
    ,SUM(CASE WHEN booking.booking_status IN ('USED', 'REIMBURSED') THEN booking.booking_quantity ELSE NULL END) AS count_used_tickets_booked
    ,SUM(CASE WHEN booking.booking_status IN ('CONFIRMED') THEN booking.booking_quantity ELSE NULL END) AS count_pending_tickets_booked
    ,COUNT(DISTINCT CASE WHEN booking.booking_status IN ('CONFIRMED') THEN booking.booking_id ELSE NULL END) AS count_pending_bookings
    ,SUM(CASE WHEN booking.booking_status IN ('USED', 'REIMBURSED') THEN booking.booking_intermediary_amount ELSE NULL END) AS real_amount_booked
    ,SUM(CASE WHEN booking.booking_status IN ('CONFIRMED') THEN booking.booking_intermediary_amount ELSE NULL END) AS pending_amount_booked
FROM {{ ref('mrt_global__venue') }} venue
JOIN {{ ref('enriched_offerer_data') }} offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
LEFT JOIN {{ ref('enriched_offer_data') }} offer ON venue.venue_id = offer.venue_id
LEFT JOIN {{ source('clean','subcategories') }} subcategories ON offer.offer_subcategoryid = subcategories.id
LEFT JOIN {{ ref('enriched_booking_data') }} booking ON offer.offer_id = booking.offer_id AND booking.booking_status IN ('USED', 'REIMBURSED', 'CONFIRMED')
WHERE offerer_siren IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
),
collective_data AS (
SELECT
    offerer_siren
    ,CASE WHEN offer.collective_offer_id IS NULL THEN NULL ELSE "COLLECTIVE" END AS individual_collective
    ,venue.venue_id
    ,venue.venue_name
    ,venue.venue_public_name
    ,offer.collective_offer_category_id
    ,offer.collective_offer_subcategory_id
    ,offer.collective_offer_id
    ,offer.collective_offer_name
    ,COUNT(DISTINCT CASE WHEN booking.collective_booking_status IN ('USED', 'REIMBURSED','CONFIRMED') THEN booking.collective_booking_id ELSE NULL END) AS count_bookings -- will be deleted
    ,COUNT(DISTINCT CASE WHEN booking.collective_booking_status IN ('USED', 'REIMBURSED','CONFIRMED') THEN booking.collective_booking_id ELSE NULL END) AS count_used_bookings
    ,COUNT(DISTINCT CASE WHEN booking.collective_booking_status IN ('PENDING') THEN booking.collective_booking_id ELSE NULL END) AS count_pending_bookingst
    ,COUNT(DISTINCT CASE WHEN booking.collective_booking_status IN ('USED', 'REIMBURSED','CONFIRMED') THEN booking.collective_booking_id ELSE NULL END) AS count_used_tickets_booked -- 1 offer = 1 ticket, just to match format
    ,COUNT(DISTINCT CASE WHEN booking.collective_booking_status IN ('PENDING') THEN booking.collective_booking_id ELSE NULL END) AS count_pending_tickets_booked -- same
    ,SUM(CASE WHEN booking.collective_booking_status IN ('USED', 'REIMBURSED','CONFIRMED') THEN booking.booking_amount ELSE NULL END) AS real_amount_booked
    ,SUM(CASE WHEN booking.collective_booking_status IN ('PENDING') THEN booking.booking_amount ELSE NULL END) AS pending_amount_booked
FROM {{ ref('mrt_global__venue') }}  venue
JOIN  {{ ref('enriched_offerer_data') }} offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
LEFT JOIN  {{ ref('enriched_collective_offer_data') }} offer ON venue.venue_id = offer.venue_id
LEFT JOIN {{ ref('enriched_collective_booking_data') }} booking ON offer.collective_offer_id = booking.collective_offer_id AND booking.collective_booking_status IN ('USED', 'REIMBURSED', 'CONFIRMED', 'PENDING')
WHERE offerer_siren IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
),
all_data AS (
SELECT
    *
FROM individual_data
UNION ALL
SELECT
    *
FROM collective_data
)
SELECT *
FROM all_data