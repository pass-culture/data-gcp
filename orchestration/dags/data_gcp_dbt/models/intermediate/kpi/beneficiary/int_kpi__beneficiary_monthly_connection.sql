with
    source_weekly_data as (
        select
            deposit_reform_category,
            deposit_id,
            nb_visits,
            deposit_amount,
            cumulative_amount_spent,
            date_trunc(active_week, month) as activity_month
        from {{ ref("aggregated_weekly_user_data") }}
        where (deposit_amount - cumulative_amount_spent) >= 5
    ),

    monthly_aggregates as (
        select
            activity_month,
            deposit_reform_category,
            count(distinct deposit_id) as total_eligible_users,
            count(
                distinct case when nb_visits > 0 then deposit_id end
            ) as total_connected_users
        from source_weekly_data
        group by activity_month, deposit_reform_category
    )

select
    activity_month,
    deposit_reform_category,
    total_eligible_users,
    total_connected_users,
    safe_divide(
        coalesce(total_connected_users, 0), coalesce(total_eligible_users, 0)
    ) as pct_connected_users
from monthly_aggregates
