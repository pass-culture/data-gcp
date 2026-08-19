with
    source_visits as (
        select
            user_id,
            first_event_date as activity_date,
            date_trunc(first_event_date, month) as activity_month
        from {{ ref("firebase_visits") }}
        where
            user_id is not null
            and first_event_date >= date_sub(current_date(), interval 36 month)
    ),

    users_metadata as (
        select user_id, user_current_deposit_reform_category as deposit_reform_category
        from {{ ref("int_global__user_beneficiary") }}
    ),

    monthly_mau as (
        select
            vis.activity_month,
            coalesce(usr.deposit_reform_category, 'unknown') as deposit_reform_category,
            count(distinct vis.user_id) as total_mau
        from source_visits as vis
        inner join users_metadata as usr on vis.user_id = usr.user_id
        group by vis.activity_month, coalesce(usr.deposit_reform_category, 'unknown')
    ),

    daily_user_activity as (
        select
            vis.activity_date,
            vis.user_id,
            date_trunc(vis.activity_date, month) as activity_month,
            coalesce(usr.deposit_reform_category, 'unknown') as deposit_reform_category
        from source_visits as vis
        inner join users_metadata as usr on vis.user_id = usr.user_id
    ),

    daily_user_dau as (
        select
            activity_date,
            activity_month,
            deposit_reform_category,
            count(distinct user_id) as total_dau
        from daily_user_activity
        group by activity_date, activity_month, deposit_reform_category
    ),

    monthly_avg_dau as (
        select activity_month, deposit_reform_category, avg(total_dau) as avg_dau
        from daily_user_dau
        group by activity_month, deposit_reform_category
    )

select
    mau.activity_month,
    mau.deposit_reform_category,
    mau.total_mau,
    round(cast(dau.avg_dau as numeric), 2) as avg_dau,
    safe_divide(
        cast(dau.avg_dau as numeric), cast(mau.total_mau as numeric)
    ) as pct_stickiness_dau_mau_ratio
from monthly_mau as mau
left join
    monthly_avg_dau as dau
    on mau.activity_month = dau.activity_month
    and mau.deposit_reform_category = dau.deposit_reform_category
