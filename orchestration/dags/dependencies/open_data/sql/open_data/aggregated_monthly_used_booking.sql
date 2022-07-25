WITH region_format AS (
SELECT 
    * except(user_region_name),
    IF(user_department_code = "99", "Étranger", user_region_name) as user_region_name

FROM  `{{ bigquery_analytics_dataset }}.aggregated_daily_used_booking`
),


SELECT
    DATE_TRUNC(day, MONTH) AS month,
    free_vs_paid_for,
    IF(user_region_name is null, -1, user_department_code) as user_department_code,
    COALESCE(user_region_name, "Inconnu") as user_region_name,
    deposit_type,
    offer_category_name,
    sum(cnt_bookings) as cnt_bookings,
    sum(amount_spent) AS amount_spent
FROM
    region_format
WHERE
    day < DATE_TRUNC(CURRENT_DATE, MONTH)
    AND deposit_type = "GRANT_18"
GROUP BY
    1, 2, 3, 4, 5, 6