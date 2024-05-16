SELECT 
    offer.isbn
    ,count(distinct booking_id) AS nb_booking
FROM `{{ bigquery_analytics_dataset }}.enriched_offer_data` offer
LEFT JOIN `{{ bigquery_analytics_dataset }}.global_booking` booking ON offer.offer_id = booking.offer_id
WHERE booking.booking_created_at >= DATE_SUB(current_date, INTERVAL 30 DAY)
    AND booking.booking_is_cancelled IS False
GROUP BY offer.isbn
HAVING offer.isbn is not null
ORDER BY nb_booking DESC