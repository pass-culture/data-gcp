SELECT
    DATE_TRUNC(day, MONTH) AS month,
    user_civility,
    user_activity,
    user_region_name,
    user_department_code,
    sum(cnt_18_users_created) as cnt_18_users_created,
    sum(cnt_15_17_users_created) AS cnt_15_17_users_created
FROM
    `{{ bigquery_analytics_dataset }}.aggregated_daily_user_data`
WHERE month < DATE_TRUNC(CURRENT_DATE, MONTH) 
GROUP BY
    month,
    user_civility,
    user_activity,
    user_region_name,
    user_department_code,