WITH all_months AS (
    SELECT
        DISTINCT DATE_TRUNC(user_deposit_creation_date, MONTH) AS current_month
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_user_data`
),
all_users AS (
    SELECT
        DISTINCT user_id,
        DATE_TRUNC(user_deposit_creation_date, MONTH) AS user_deposit_creation_month
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_user_data`
),
user_diversification AS (
    SELECT
        DATE_TRUNC(user_deposit_creation_date, MONTH) as user_deposit_creation_month,
        DATE_TRUNC(booking_creation_date, MONTH) as booking_creation_month,
        user_id,
        sum(booking_amount) as booking_amount,
        sum(delta_diversification) as delta_diversification,
        count(distinct booking_id) as distinct_booking
    FROM
        `{{ bigquery_analytics_dataset }}.diversification_booking`
    GROUP BY
        1,
        2,
        3
),
all_users_and_month AS (
    SELECT
        user_id,
        user_deposit_creation_month,
        current_month,
        DATE_DIFF(
            DATE(current_month),
            DATE(user_deposit_creation_month),
            MONTH
        ) AS seniority
    FROM
        all_users
        CROSS JOIN all_months
    WHERE
        DATE(current_month) >= DATE(user_deposit_creation_month)
)
SELECT
    au.user_id,
    au.user_deposit_creation_month,
    au.current_month,
    seniority,
    coalesce(booking_amount, 0) as booking_amount,
    coalesce(delta_diversification, 0) as delta_diversification,
    coalesce(distinct_booking, 0) as distinct_booking,
    sum(distinct_booking) over(PARTITION BY au.user_id) as total_booking,
    sum(booking_amount) over(PARTITION BY au.user_id) as total_booking_amount,
    sum(delta_diversification) over(PARTITION BY au.user_id) as total_delta_diversification
FROM
    all_users_and_month au
    LEFT JOIN user_diversification ud on ud.user_id = au.user_id
    AND ud.user_deposit_creation_month = au.user_deposit_creation_month
    AND ud.booking_creation_month = au.current_month