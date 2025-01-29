-- TODO: deprecated

select
    date_trunc(active_date, month) as active_month,
    months_since_deposit_created,
    user_id,
    user_department_code,
    user_region_name,
    deposit_id,
    deposit_type,
    seniority_months,
    max(cumulative_amount_spent) as cumulative_amount_spent,
    max(cumulative_cnt_used_bookings) as cumulative_cnt_used_bookings
from {{ ref("aggregated_daily_user_used_activity") }}
group by
    active_month,
    months_since_deposit_created,
    user_id,
    user_department_code,
    user_region_name,
    deposit_id,
    deposit_type,
    seniority_months
