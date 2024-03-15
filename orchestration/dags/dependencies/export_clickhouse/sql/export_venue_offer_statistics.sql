SELECT  
    DATE('{{ ds }}') as update_date,
    offerer_siren as offerer_siren,
    individual_collective as individual_collective,
    venue_id as venue_id,
    venue_name as venue_name,
    venue_public_name as venue_public_name,
    category_id as category_id,
    subcategory as subcategory,
    offer_id as offer_id,
    offer_name as offer_name,
    count_bookings,
    count_used_bookings,
    count_used_tickets_booked,
    count_pending_tickets_booked,
    count_pending_bookings,
    real_amount_booked,
    pending_amount_booked
FROM `{{ bigquery_analytics_prod }}.venue_siren_offers` 