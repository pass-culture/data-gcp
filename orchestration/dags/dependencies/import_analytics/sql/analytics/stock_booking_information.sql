
SELECT
    stock.stock_id
    , COALESCE(SUM(booking.booking_quantity),0) AS booking_quantity
    , COALESCE(SUM(CASE WHEN booking.booking_status = 'CANCELLED' THEN booking.booking_quantity ELSE NULL END),0) AS booking_cancelled
    , COALESCE(SUM(CASE WHEN booking.booking_status != 'CANCELLED' THEN booking.booking_quantity ELSE NULL END),0) AS booking_non_cancelled
    , COALESCE(SUM(CASE WHEN booking.booking_status = 'REIMBURSED' THEN booking.booking_quantity ELSE NULL END),0) AS bookings_paid
FROM
    `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock
    LEFT JOIN `{{ bigquery_clean_dataset }}`.booking AS booking ON booking.stock_id = stock.stock_id
GROUP BY
    stock.stock_id