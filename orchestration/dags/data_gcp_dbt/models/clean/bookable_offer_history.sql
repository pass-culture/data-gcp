WITH bookings_per_stock AS (
    SELECT
        stock_id,
        partition_date,
        COUNT(
            DISTINCT CASE
                WHEN booking_status NOT IN ('CANCELLED') THEN booking_id
                ELSE NULL
            END
        ) AS booking_stock_no_cancelled_cnt
    FROM
        {{ref('booking_history')}} AS booking
    WHERE partition_date = PARSE_DATE('%Y-%m-%d','{{ ds() }}')

    GROUP BY
        stock_id,
        partition_date
)
SELECT
    DISTINCT stock.partition_date, stock.offer_id
FROM
    {{ref('stock_history')}} AS stock
    JOIN {{ref('offer_history')}} AS offer ON stock.offer_id = offer.offer_id
    AND stock.partition_date = offer.partition_date
    AND offer.offer_is_active
    AND NOT stock.stock_is_soft_deleted
    LEFT JOIN bookings_per_stock ON stock.stock_id = bookings_per_stock.stock_id
    AND stock.partition_date = bookings_per_stock.partition_date
WHERE
    (
        (
            DATE(stock.stock_booking_limit_date) > stock.partition_date
            OR stock.stock_booking_limit_date IS NULL
        )
        AND (
            DATE(stock.stock_beginning_date) > stock.partition_date
            OR stock.stock_beginning_date IS NULL
        )
        AND offer.offer_is_active
        AND (
            stock.stock_quantity IS NULL
            OR GREATEST(
                stock.stock_quantity - COALESCE(
                    bookings_per_stock.booking_stock_no_cancelled_cnt,
                    0
                ),
                0
            ) > 0
        )
    )

    AND stock.partition_date = DATE('{{ ds() }}')

