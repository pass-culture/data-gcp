SELECT
    stock.offer_id
    , offer.*
FROM `{{ bigquery_raw_dataset }}`.applicative_database_stock AS stock
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer 
    ON stock.offer_id = offer.offer_id
WHERE (
    DATE(enriched_stock.stock_booking_limit_date) > CURRENT_DATE
    OR enriched_stock.stock_booking_limit_date IS NULL
    )
AND (
    DATE(enriched_stock.stock_beginning_date) > CURRENT_DATE
    OR enriched_stock.stock_beginning_date IS NULL
    )
AND offer.offer_is_active
AND (
    available_stock_information > 0
    OR available_stock_information IS NULL
    )
AND NOT stock_is_soft_deleted
AND offer_validation = 'APPROVED'