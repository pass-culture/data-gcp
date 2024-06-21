SELECT 
    offer.isbn
    ,count(distinct booking_id) AS nb_booking
FROM {{ ref('mrt_global__offer') }} offer
LEFT JOIN {{ ref('mrt_global__booking') }} booking ON offer.offer_id = booking.offer_id
WHERE booking.booking_created_at >= DATE_SUB(current_date, INTERVAL 30 DAY)
    AND booking.booking_is_cancelled IS False
GROUP BY offer.isbn
HAVING offer.isbn is not null
ORDER BY nb_booking DESC