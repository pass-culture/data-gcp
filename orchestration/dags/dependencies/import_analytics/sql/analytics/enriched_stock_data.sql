
{{ create_humanize_id_function() }} 

WITH stock_humanized_id AS (
SELECT
    stock_id,
    humanize_id(stock_id) AS humanized_id
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_stock
WHERE
    stock_id is not NULL
)
       
SELECT
    stock.stock_id,
    stock.offer_id,
    offer.offer_name,
    venue.venue_managing_offerer_id AS offerer_id,
    offer.offer_subcategoryId,
    venue.venue_department_code,
    stock.stock_creation_date,
    stock.stock_booking_limit_date,
    stock.stock_beginning_date,
    available_stock_information.available_stock_information,
    stock.stock_quantity,
    stock_booking_information.booking_quantity,
    stock_booking_information.bookings_cancelled AS booking_cancelled,
    stock_booking_information.bookings_paid AS booking_paid,
    stock.stock_price
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON venue.venue_id = offer.venue_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.stock_booking_information ON stock.stock_id = stock_booking_information.stock_id
    LEFT JOIN stock_humanized_id AS stock_humanized_id ON stock_humanized_id.stock_id = stock.stock_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.available_stock_information ON stock_booking_information.stock_id = available_stock_information.stock_id