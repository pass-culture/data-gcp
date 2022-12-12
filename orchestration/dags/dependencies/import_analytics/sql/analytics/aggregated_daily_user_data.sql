SELECT
    DATE(edd.deposit_creation_date) AS day,
    eud.user_civility,
    eud.user_activity,
    eud.user_region_name,
    eud.user_department_code,
    COUNT(
        DISTINCT CASE
            WHEN edd.deposit_type = 'GRANT_18' THEN edd.user_id
            ELSE NULL
        END
    ) AS cnt_18_users_created,
    COUNT(
        DISTINCT CASE
            WHEN edd.deposit_type = 'GRANT_15_17' THEN edd.user_id
            ELSE NULL
        END
    ) AS cnt_15_17_users_created
FROM
    `{{ bigquery_analytics_dataset }}.enriched_deposit_data` edd
    JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON edd.user_id = eud.user_id
GROUP BY
    day,
    eud.user_civility,
    eud.user_activity,
    eud.user_region_name,
    eud.user_department_code