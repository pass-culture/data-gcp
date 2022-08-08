
WITH last_status AS (
    SELECT
        DISTINCT (payment_status.paymentId),
        payment_status.paymentId as payment_id,
        payment_status.status,
        date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_payment_status AS payment_status
    ORDER BY
        payment_status.paymentId,
        date DESC
),
valid_payment AS (
    SELECT
        bookingId
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_payment AS payment
        LEFT JOIN last_status ON last_status.payment_id = payment.id
    WHERE
        last_status.status != 'BANNED'
),
booking_with_payment AS (
    SELECT
        booking.booking_id,
        booking.booking_quantity
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
    WHERE
        booking.booking_id IN(
            SELECT
                bookingId
            FROM
                valid_payment
        )
)
SELECT
    stock.stock_id,
    COALESCE(SUM(booking.booking_quantity), 0) AS booking_quantity,
    COALESCE(
        SUM(
            booking.booking_quantity * CAST(booking.booking_is_cancelled AS INT64)
        ),
        0
    ) AS bookings_cancelled,
    COALESCE(SUM(booking_with_payment.booking_quantity), 0) AS bookings_paid
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.stock_id = stock.stock_id
    LEFT JOIN booking_with_payment ON booking_with_payment.booking_id = booking.booking_id
GROUP BY
    stock.stock_id