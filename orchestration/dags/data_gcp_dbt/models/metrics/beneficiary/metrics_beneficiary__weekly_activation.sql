with
    onboarding_kpis as (
        select
            signup_week,
            total_onboarding_users,
            total_validated_7d_users,
            safe_cast(age_at_onboarding as int64) as age_at_signup
        from {{ ref("int_kpi__beneficiary_weekly_onboarding") }}
    ),

    activation_kpis as (
        select
            signup_week,
            age_at_signup,
            total_registered_beneficiaries,
            total_activated_7d_beneficiaries
        from {{ ref("int_kpi__beneficiary_weekly_activation") }}
    )

select
    coalesce(onb.signup_week, act.signup_week) as signup_week,
    coalesce(onb.age_at_signup, act.age_at_signup) as age_at_signup,
    coalesce(onb.total_onboarding_users, 0) as total_onboarding_users,
    coalesce(onb.total_validated_7d_users, 0) as total_validated_7d_users,
    coalesce(act.total_registered_beneficiaries, 0) as total_registered_beneficiaries,
    coalesce(
        act.total_activated_7d_beneficiaries, 0
    ) as total_activated_7d_beneficiaries,
    coalesce(
        safe_divide(
            act.total_activated_7d_beneficiaries, act.total_registered_beneficiaries
        ),
        0
    ) as pct_activated_7d_beneficiaries,
    coalesce(
        safe_divide(onb.total_validated_7d_users, onb.total_onboarding_users), 0
    ) as pct_validated_7d_users
from onboarding_kpis as onb
full outer join
    activation_kpis as act
    on onb.signup_week = act.signup_week
    and onb.age_at_signup = act.age_at_signup
