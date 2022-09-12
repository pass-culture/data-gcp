SELECT
    DATE(booking_used_date) AS day,
    CASE
        WHEN booking_amount = 0 THEN "Gratuit"
        ELSE "Payant"
    END AS free_vs_paid_for,
    user_activity,
    user_department_code,
    region_name AS user_region_name,
    deposit_type,
    offer_category_id AS offer_category_name,
    COUNT(booking_id) AS cnt_bookings,
    SUM(
        CASE
            WHEN booking_used_date IS NOT NULL THEN booking_intermediary_amount
            ELSE NULL
        END
    ) AS amount_spent,
    COUNT(DISTINCT user_id) AS cnt_users
FROM
    `{{ bigquery_analytics_dataset }}.enriched_booking_data`
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` ON `{{ bigquery_analytics_dataset }}.region_department`.num_dep = `{{ bigquery_analytics_dataset }}.enriched_booking_data`.user_department_code
WHERE
    `{{ bigquery_analytics_dataset }}.enriched_booking_data`.booking_is_used
GROUP BY
    day,
    user_activity,
    user_department_code,
    user_region_name,
    deposit_type,
    free_vs_paid_for,
    offer_category_name