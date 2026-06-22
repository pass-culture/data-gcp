with
    generic_attributes as (
        select
            user_id,
            user_information_created_at,
            'activity' as user_information_type,
            user_activity as user_information_value,
            user_previous_activity as user_previous_information_value
        from {{ ref("int_history__user_beneficiary_information_history") }}

    )

select
    user_id,
    user_information_type,
    user_information_value,
    user_information_created_at as user_information_start_date,
    lead(user_information_created_at) over (
        partition by user_id, user_information_type order by user_information_created_at
    ) as user_information_end_date

from generic_attributes
where
    (
        user_information_value != user_previous_information_value
        or user_previous_information_value is null
    )
