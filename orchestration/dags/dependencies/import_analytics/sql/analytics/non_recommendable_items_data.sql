SELECT
    DISTINCT user_id AS user_id,
    item_id AS item_id
FROM
    `{{ bigquery_analytics_dataset }}.enriched_booking_data` b
    JOIN `{{ bigquery_analytics_dataset }}.offer_item_ids` o on o.offer_id = b.offer_id
WHERE
    booking_is_cancelled = false