SELECT
    collective_stock_id,
    stock_id,
    collective_stock_creation_date,
    collective_stock_modification_date,
    collective_stock_beginning_date_time,
    collective_offer_id,
    collective_stock_price,
    collective_stock_booking_limit_date_time,
    collective_stock_number_of_tickets,
    collective_stock_price_detail,
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM {{ source('raw', 'applicative_database_collective_stock') }}
