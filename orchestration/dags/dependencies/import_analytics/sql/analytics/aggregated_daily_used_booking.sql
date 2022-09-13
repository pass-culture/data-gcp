SELECT
    DATE(booking_used_date) AS day,
    CASE
        WHEN booking_amount = 0 THEN "Gratuit"
        ELSE "Payant"
    END AS free_vs_paid_for,
    `{{ bigquery_analytics_dataset }}.enriched_user_data`.user_activity,
    `{{ bigquery_analytics_dataset }}.enriched_user_data`.user_department_code,
    `{{ bigquery_analytics_dataset }}.enriched_user_data`.user_region_name,
    `{{ bigquery_analytics_dataset }}.enriched_booking_data`.deposit_type,
    offer_category_id AS offer_category_name,
    CASE
        WHEN `{{ bigquery_analytics_dataset }}.enriched_booking_data`.digital_goods THEN "Produits numériques"
        ELSE "Biens et services"
    END AS product_type,
    COUNT(booking_id) AS cnt_bookings,
    SUM(
        CASE
            WHEN booking_used_date IS NOT NULL THEN booking_intermediary_amount
            ELSE NULL
        END
    ) AS amount_spent,
    COUNT(DISTINCT `{{ bigquery_analytics_dataset }}.enriched_booking_data`.user_id) AS cnt_users
FROM
    `{{ bigquery_analytics_dataset }}.enriched_booking_data`
    JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` ON `{{ bigquery_analytics_dataset }}.enriched_booking_data`.user_id = `{{ bigquery_analytics_dataset }}.enriched_user_data`.user_id
WHERE
    `{{ bigquery_analytics_dataset }}.enriched_booking_data`.booking_is_used
GROUP BY
    day,
    user_activity,
    user_department_code,
    user_region_name,
    deposit_type,
    free_vs_paid_for,
    offer_category_name,
    product_type