WITH bookings_per_stock AS (
    SELECT
        collective_stock_id,
        partition_date,
        COUNT(
            DISTINCT CASE
                WHEN collective_booking_status NOT IN ('CANCELLED') THEN collective_booking_id
                ELSE NULL
            END
        ) AS collective_booking_stock_no_cancelled_cnt
    FROM
        {{ ref('collective_booking_history')}} AS collective_booking
    WHERE partition_date = PARSE_DATE('%Y-%m-%d','{{ ds() }}')
    GROUP BY
        1,
        2
)
SELECT
    DISTINCT collective_stock.collective_offer_id,
    collective_stock.partition_date,
    FALSE AS collective_offer_is_template
FROM
    {{ ref('collective_stock_history')}} AS collective_stock
    JOIN {{ ref('collective_offer_history')}} AS collective_offer ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
    AND collective_offer.collective_offer_is_active
    AND collective_offer.partition_date = collective_stock.partition_date
    AND collective_offer_validation = "APPROVED"
    LEFT JOIN bookings_per_stock ON collective_stock.collective_stock_id = bookings_per_stock.collective_stock_id
    AND collective_stock.partition_date = bookings_per_stock.partition_date
WHERE
    (
        (
            DATE(
                collective_stock.collective_stock_booking_limit_date_time
            ) > collective_stock.partition_date
            OR collective_stock.collective_stock_booking_limit_date_time IS NULL
        )
        AND (
            DATE(
                collective_stock.collective_stock_beginning_date_time
            ) > collective_stock.partition_date
            OR collective_stock.collective_stock_beginning_date_time IS NULL
        )
        AND collective_offer.collective_offer_is_active
        AND (
            collective_booking_stock_no_cancelled_cnt IS NULL
        )
    )
    AND collective_stock.partition_date = PARSE_DATE('%Y-%m-%d','{{ ds() }}')
UNION ALL
SELECT
    collective_offer_template.collective_offer_id
    ,collective_offer_template.partition_date
    ,TRUE AS collective_offer_is_template
FROM
        {{ ref('collective_offer_template_history')}} AS collective_offer_template
    WHERE partition_date = PARSE_DATE('%Y-%m-%d','{{ ds() }}')
    AND collective_offer_validation = "APPROVED"
