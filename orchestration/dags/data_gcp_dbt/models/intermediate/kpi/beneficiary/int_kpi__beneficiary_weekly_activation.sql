with
    source_users as (
        select
            user_id,
            user_creation_date,
            user_birth_date,
            user_age_at_creation as age_at_signup,
            date_trunc(date(user_creation_date), week(monday)) as signup_week
        from {{ ref("int_applicative__user") }}
    ),

    fraud_checks as (
        select distinct user_id
        from {{ ref("int_applicative__beneficiary_fraud_check") }}
        where
            reasoncodes in (
                'AGE_NOT_VALID',
                'AGE_TOO_OLD',
                'AGE_TOO_YOUNG',
                'DUPLICATE_ID_PIECE_NUMBER',
                'DUPLICATE_INE',
                'NOT_ELIGIBLE'
            )
    ),

    beneficiaries as (
        select user_id, first_deposit_creation_date
        from {{ ref("int_global__user_beneficiary") }}
    ),

    eligible_user_activations as (
        select
            usr.user_id,
            usr.signup_week,
            usr.age_at_signup,
            case
                when
                    ben.first_deposit_creation_date is not null
                    and date_diff(
                        date(ben.first_deposit_creation_date),
                        date(usr.user_creation_date),
                        day
                    )
                    <= 7
                then 1
                else 0
            end as is_activated_within_7d
        from source_users as usr
        left join fraud_checks as fraud on usr.user_id = fraud.user_id
        left join beneficiaries as ben on usr.user_id = ben.user_id
        where fraud.user_id is null and usr.age_at_signup between 15 and 18
    )

select
    signup_week,
    age_at_signup,
    coalesce(count(distinct user_id), 0) as total_registered_beneficiaries,
    coalesce(
        count(distinct case when is_activated_within_7d = 1 then user_id end), 0
    ) as total_activated_7d_beneficiaries
from eligible_user_activations
where age_at_signup between 15 and 18
group by signup_week, age_at_signup
