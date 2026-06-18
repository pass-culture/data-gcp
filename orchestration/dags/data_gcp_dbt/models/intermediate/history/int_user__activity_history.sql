{{
    config(
        materialized="table",
    )
}}
select
    user_id,
    user_activity,
    user_information_created_at as start_date,
    lead(user_information_created_at) over (
        partition by user_id order by user_information_created_at
    ) as end_date

from {{ ref("int_history__user_beneficiary_information_history") }}
where (user_activity != user_previous_activity or user_previous_activity is null)
