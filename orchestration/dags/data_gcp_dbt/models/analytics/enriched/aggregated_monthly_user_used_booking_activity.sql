SELECT
    DATE_TRUNC(active_date,MONTH) AS active_month,
    months_since_deposit_created,
    user_id,
    user_department_code,
    user_region_name,
    deposit_id,
    deposit_type,
    seniority_months,
    MAX(cumulative_amount_spent) AS cumulative_amount_spent,
    MAX(cumulative_cnt_used_bookings) AS cumulative_cnt_used_bookings
FROM
    {{ ref('aggregated_daily_user_used_activity') }}
GROUP BY
    active_month,
    months_since_deposit_created,
    user_id,
    user_department_code,
    user_region_name,
    deposit_id,
    deposit_type,
    seniority_months