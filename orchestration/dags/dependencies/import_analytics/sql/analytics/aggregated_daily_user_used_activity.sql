WITH days AS (
    SELECT
        *
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY('2019-02-11', CURRENT_DATE, INTERVAL 1 DAY)
        ) AS day
),
user_active_dates AS (
    SELECT
        user_id,
        user_department_code,
        user_region_name,
        deposit_id,
        deposit_amount,
        deposit_creation_date,
        deposit_type,
        days.day AS active_date,
        DATE_DIFF(CURRENT_DATE, deposit_creation_date, DAY) AS seniority_days,
        DATE_DIFF(CURRENT_DATE, deposit_creation_date, MONTH) AS seniority_months
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
        JOIN days ON days.day BETWEEN `{{ bigquery_analytics_dataset }}.enriched_deposit_data`.deposit_creation_date
        AND `{{ bigquery_analytics_dataset }}.enriched_deposit_data`.deposit_expiration_date
),
aggregated_daily_user_used_bookings_history_1 AS (
    SELECT
        user_active_dates.active_date,
        user_active_dates.user_id,
        user_active_dates.user_department_code,
        user_active_dates.user_region_name,
        user_active_dates.deposit_id,
        user_active_dates.deposit_type,
        user_active_dates.deposit_amount AS initial_deposit_amount,
        seniority_days,
        seniority_months,
        DATE_DIFF(
            user_active_dates.active_date,
            deposit_creation_date,
            DAY
        ) AS days_since_deposit_created,
        DATE_DIFF(
            user_active_dates.active_date,
            deposit_creation_date,
            MONTH
        ) AS months_since_deposit_created,
        COALESCE(SUM(booking_intermediary_amount), 0) AS amount_spent,
        COUNT(booking_id) AS cnt_used_bookings
    FROM
        user_active_dates
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd ON ebd.deposit_id = user_active_dates.deposit_id
        AND user_active_dates.active_date = DATE(booking_used_date)
        AND booking_is_used
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
)
SELECT
    active_date,
    user_id,
    user_department_code,
    user_region_name,
    deposit_id,
    deposit_type,
    initial_deposit_amount,
    seniority_days,
    seniority_months,
    days_since_deposit_created,
    months_since_deposit_created,
    amount_spent,
    cnt_used_bookings,
    SUM(amount_spent) OVER (
        PARTITION BY user_id,
        deposit_id
        ORDER BY
            active_date ASC
    ) AS cumulative_amount_spent,
    SUM(cnt_used_bookings) OVER (
        PARTITION BY user_id,
        deposit_id
        ORDER BY
            active_date
    ) AS cumulative_cnt_used_bookings,
FROM
    aggregated_daily_user_used_bookings_history_1