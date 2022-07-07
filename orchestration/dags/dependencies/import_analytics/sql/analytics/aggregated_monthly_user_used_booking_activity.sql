SELECT
    active_date,
    months_since_deposit_created,
    user_id,
    deposit_id,
    deposit_type,
    seniority_months,
    MAX(cumulative_amount_spent) AS cumulative_amount_spent,
    MAX(cumulative_cnt_used_bookings) AS cumulative_cnt_used_bookings
FROM
    `{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity`
GROUP BY
    active_date,
    months_since_deposit_created,
    user_id,
    deposit_id,
    deposit_type,
    seniority_months