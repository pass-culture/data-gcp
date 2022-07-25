WITH region_format AS (
SELECT 
    * except(user_region_name),
    IF(user_department_code = "99", "Étranger", user_region_name) as user_region_name

FROM  `{{ bigquery_analytics_dataset }}.aggregated_daily_user_data`
)

SELECT
    DATE_TRUNC(day, MONTH) AS month,
    user_civility,
    user_activity,
    IF(user_region_name is null, "-1", user_department_code) as user_department_code,
    COALESCE(user_region_name, "Non Communiqué") as user_region_name,
    sum(cnt_18_users_created) as cnt_18_users_created
FROM
   region_format
WHERE day < DATE_TRUNC(CURRENT_DATE, MONTH) 
GROUP BY
    1, 2, 3, 4, 5