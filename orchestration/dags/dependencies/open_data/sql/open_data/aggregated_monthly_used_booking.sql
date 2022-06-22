SELECT
    DATE_TRUNC(day, MONTH) AS month,
    free_vs_paid_for,
    user_department_code,
    user_region_name,
    deposit_type,
    offer_category_name,
    sum(cnt_bookings) as cnt_bookings,
    sum(amount_spent) AS amount_spent
FROM
    `{{ bigquery_analytics_dataset }}.aggregated_daily_used_booking`
WHERE month < DATE_TRUNC(CURRENT_DATE, MONTH) 
GROUP BY
    month,
    free_vs_paid_for,
    user_department_code,
    user_region_name,
    deposit_type,
    offer_category_name