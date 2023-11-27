WITH bookings_grouped_by_stock AS (
    SELECT
        booking.stock_id,
        SUM(booking.booking_quantity) as number_of_booking
    FROM
        {{ ref('applicative_database_booking') }} AS booking
    LEFT JOIN {{ ref('applicative_database_stock') }} AS stock 
        ON booking.stock_id = stock.stock_id
    WHERE
        booking.booking_is_cancelled = False
    GROUP BY
        booking.stock_id
)
SELECT
    stock.stock_id,
    CASE
        WHEN stock.stock_quantity IS NULL THEN NULL
        ELSE GREATEST(
            stock.stock_quantity - COALESCE(bookings_grouped_by_stock.number_of_booking, 0),
            0
        )
    END AS available_stock_information
FROM
    {{ ref('applicative_database_stock') }} AS stock
    LEFT JOIN bookings_grouped_by_stock ON bookings_grouped_by_stock.stock_id = stock.stock_id