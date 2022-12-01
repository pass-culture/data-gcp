WITH region_format AS (
    SELECT 
        * except(user_region_name),
        IF(user_department_code = "99", null, user_region_name) as user_region_name

    FROM  `{{ bigquery_analytics_dataset }}.aggregated_daily_user_data`
    WHERE day >= DATE("2019-02-01")
)

SELECT
    DATE_TRUNC(day, MONTH) AS month,
     CASE 
            WHEN user_civility = 'M.' THEN "Homme"
            WHEN user_civility = 'Mme' THEN "Femme"
            ELSE "Non Renseigné"
    END as user_civility,
    IF(user_region_name is null, "Non Renseigné", user_department_code) as user_department_code,
    COALESCE(user_region_name, "Non Renseigné") as user_region_name,
    sum(cnt_18_users_created) as cnt_18_users_created
FROM
   region_format
WHERE day < DATE_TRUNC(CURRENT_DATE, MONTH) 
GROUP BY
    1, 2, 3, 4