with selected_users as (
    select
        eu.user_id,
        eu.first_deposit_creation_date as user_deposit_creation_date,
        eu.user_birth_date,
        eu.first_deposit_amount as user_deposit_initial_amount,
        eu.last_deposit_amount as user_last_deposit_amount,
        eu.total_theoretical_remaining_credit,
        eu.total_individual_bookings as booking_cnt
    from
        {{ ref('mrt_global__user') }} eu
    union all
    select
        ie.user_id,
        null as user_deposit_creation_date,
        u.user_birth_date,
        null as user_deposit_initial_amount,
        null as user_last_deposit_amount,
        null as user_theoretical_remaining_credit,
        0 as booking_cnt
    from
        {{ source('raw', 'applicative_database_internal_user') }} ie
        left join {{ source('raw', 'applicative_database_user') }} u on u.user_id = ie.user_id
)


select
    eu.user_id,
    eu.user_deposit_creation_date,
    eu.user_birth_date,
    eu.user_deposit_initial_amount,
    coalesce(eu.total_theoretical_remaining_credit, eu.user_last_deposit_amount) as user_theoretical_remaining_credit,
    eu.booking_cnt,
    au.consult_offer,
    au.has_added_offer_to_favorites
from selected_users eu
    left join {{ ref('firebase_aggregated_users') }} au on eu.user_id = au.user_id
qualify row_number() over (partition by eu.user_id order by eu.booking_cnt desc) = 1
