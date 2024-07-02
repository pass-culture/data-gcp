SELECT
    offer.*
    , offerer.offerer_id
    , null as test
FROM {{ source('raw','applicative_database_stock') }} AS stock
JOIN {{ ref('offer') }} AS offer 
    ON stock.offer_id = offer.offer_id
LEFT JOIN {{ ref('available_stock_information') }} AS av_stock
    ON stock.stock_id = av_stock.stock_id
LEFT JOIN {{ source('raw','applicative_database_venue') }} AS venue
    ON offer.venue_id = venue.venue_id
LEFT JOIN {{ source('raw','applicative_database_offerer') }} AS offerer
    ON venue.venue_managing_offerer_id = offerer.offerer_id
WHERE (
    DATE(stock.stock_booking_limit_date) > CURRENT_DATE
    OR stock.stock_booking_limit_date IS NULL
    )
AND (
    DATE(stock.stock_beginning_date) > CURRENT_DATE
    OR stock.stock_beginning_date IS NULL
    )
AND offer.offer_is_active
AND (
    available_stock_information > 0
    OR available_stock_information IS NULL
    )
AND NOT stock_is_soft_deleted
AND offer_validation = 'APPROVED'
limit 10