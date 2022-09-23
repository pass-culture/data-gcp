SELECT
    DISTINCT b.user_id AS user_id,
    s.offer_id AS offer_id
FROM
    `{{ bigquery_clean_dataset }}.applicative_database_booking` b
    INNER JOIN `{{ bigquery_clean_dataset }}.applicative_database_stock` s ON b.stock_id = s.stock_id
WHERE
    b.booking_is_cancelled = false