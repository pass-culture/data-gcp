{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'partition_date', 'data_type': 'date'},
    )
) }}

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
        {{ source('clean','applicative_database_booking_history') }} AS booking
    {% if is_incremental() %}
    WHERE partition_date = DATE_SUB('{{ ds() }}', INTERVAL 1 DAY)
    {% endif %}
    GROUP BY
        stock_id,
        partition_date
)
SELECT
    DISTINCT stock.partition_date, stock.offer_id, offer_item_ids.item_id, offer.offer_subcategoryId AS offer_subcategory_id, subcategories.category_id AS offer_category_id
FROM
    {{ source('clean','applicative_database_stock_history') }} AS stock
    JOIN {{ source('clean','applicative_database_offer_history') }} AS offer ON stock.offer_id = offer.offer_id
    AND stock.partition_date = offer.partition_date
    AND offer.offer_is_active
    AND NOT stock.stock_is_soft_deleted
    LEFT JOIN bookings_per_stock ON stock.stock_id = bookings_per_stock.stock_id
    AND stock.partition_date = bookings_per_stock.partition_date
    LEFT JOIN {{ ref('offer_item_ids') }} offer_item_ids ON offer_item_ids.offer_id = stock.offer_id
    LEFT JOIN {{ source('clean', 'subcategories') }} subcategories ON subcategories.id = offer.offer_subcategoryId
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
    {% if is_incremental() %}
    AND stock.partition_date = DATE_SUB('{{ ds() }}', INTERVAL 1 DAY)
    {% endif %}
