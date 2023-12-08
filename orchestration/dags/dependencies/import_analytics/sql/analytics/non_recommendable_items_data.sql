SELECT
    DISTINCT user_id AS user_id,
    item_id AS item_id
FROM
    `{{ bigquery_analytics_dataset }}.enriched_booking_data` b
    JOIN `{{ bigquery_analytics_dataset }}.offer_item_ids` o on o.offer_id = b.offer_id
    JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud on eud.user_id = b.user_id
    and eud.user_is_current_beneficiary
WHERE
    booking_is_cancelled = false