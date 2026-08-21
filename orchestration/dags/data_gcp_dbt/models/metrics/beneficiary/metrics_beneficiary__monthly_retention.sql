with
    monthly_connection as (
        select
            activity_month,
            deposit_reform_category,
            total_eligible_users,
            total_connected_users,
            pct_connected_users
        from {{ ref("int_kpi__beneficiary_monthly_connection") }}
    ),

    monthly_activity as (
        select
            activity_month,
            deposit_reform_category,
            total_mau,
            avg_dau,
            pct_stickiness_dau_mau_ratio
        from {{ ref("int_kpi__beneficiary_monthly_retention") }}
    )

select
    act.pct_stickiness_dau_mau_ratio,
    conn.pct_connected_users,
    coalesce(conn.activity_month, act.activity_month) as activity_month,
    coalesce(
        conn.deposit_reform_category, act.deposit_reform_category
    ) as deposit_reform_category,
    coalesce(act.total_mau, 0) as total_mau,
    coalesce(act.avg_dau, 0) as avg_dau,
    coalesce(conn.total_eligible_users, 0) as total_eligible_users,
    coalesce(conn.total_connected_users, 0) as total_connected_users
from monthly_connection as conn
full outer join
    monthly_activity as act
    on conn.activity_month = act.activity_month
    and conn.deposit_reform_category = act.deposit_reform_category
