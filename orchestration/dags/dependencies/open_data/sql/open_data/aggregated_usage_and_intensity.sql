WITH aggregated_monthly_user_used_bookings_activity AS (
    SELECT
        *
    FROM
        `{{ bigquery_analytics_dataset }}.aggregated_monthly_user_used_bookings_activity`
    WHERE months_since_deposit_created < DATE_TRUNC(CURRENT_DATE, MONTH) 
)
SELECT

    COUNT(
        CASE
            WHEN months_since_deposit_created = 3
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi1,
    COUNT(
        CASE
            WHEN months_since_deposit_created = 12
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi2,
    COUNT(
        CASE
            WHEN months_since_deposit_created = 24
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi3,
    COUNT(
        CASE
            WHEN months_since_deposit_created = 3
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi4,
    COUNT(
        CASE
            WHEN months_since_deposit_created = 12
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi5,
    COUNT(
        CASE
            WHEN months_since_deposit_created = 24
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi6,
    AVG(
        CASE
            WHEN months_since_deposit_created = 3
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS intens_kpi1,
    AVG(
        CASE
            WHEN months_since_deposit_created = 12
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS intens_kpi2,
    AVG(
        CASE
            WHEN months_since_deposit_created = 24
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS intens_kpi3
FROM
    aggregated_monthly_user_used_bookings_activity