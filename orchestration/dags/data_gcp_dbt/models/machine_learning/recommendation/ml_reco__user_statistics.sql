with
    selected_users as (
        select
            eu.user_id,
            eu.first_deposit_creation_date as user_deposit_creation_date,
            eu.user_birth_date,
            eu.first_deposit_amount as user_deposit_initial_amount,
            eu.last_deposit_amount as user_last_deposit_amount,
            eu.total_theoretical_remaining_credit,
            eu.total_deposit_amount,
            eu.total_actual_amount_spent,
            eu.total_non_cancelled_individual_bookings as booking_cnt,
            coalesce(
                eu.total_deposit_amount - eu.total_actual_amount_spent,
                eu.last_deposit_amount
            ) as user_theoretical_remaining_credit

        from {{ ref("mrt_global__user_beneficiary") }} as eu
        where
            eu.last_deposit_amount is not null  -- Bad quality data (4 rows in stg and prod)
            and eu.user_is_active  -- is compliant to CGU
            and eu.user_is_current_beneficiary
            and eu.total_deposit_amount - eu.total_actual_amount_spent > 0
    )

select
    selected_users.user_id,
    selected_users.user_deposit_creation_date,
    selected_users.user_birth_date,
    selected_users.user_deposit_initial_amount,
    selected_users.user_theoretical_remaining_credit,
    selected_users.booking_cnt,
    au.consult_offer,
    au.has_added_offer_to_favorites
from selected_users
left join
    {{ ref("firebase_aggregated_users") }} as au on selected_users.user_id = au.user_id
qualify
    row_number() over (
        partition by selected_users.user_id order by selected_users.booking_cnt desc
    )
    = 1
