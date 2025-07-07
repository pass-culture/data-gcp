with

    selected_users as (
        select
            user_id,
            first_deposit_creation_date as user_deposit_creation_date,
            user_birth_date,
            first_deposit_amount as user_deposit_initial_amount,
            coalesce(
                total_theoretical_remaining_credit, last_deposit_amount
            ) as user_theoretical_remaining_credit
        from {{ ref("mrt_global__user") }}
    ),

    firebase_aggregated_users as (
        select
            user_id,
            has_added_offer_to_favorites as user_favorites_count,
            consult_offer as user_clicks_count,
            booking_confirmation as user_bookings_count
        from {{ ref("firebase_aggregated_users") }}
    )

select
    selected_users.user_id,
    selected_users.user_deposit_creation_date,
    selected_users.user_birth_date,
    selected_users.user_deposit_initial_amount,
    selected_users.user_theoretical_remaining_credit,
    firebase_aggregated_users.user_clicks_count,
    firebase_aggregated_users.user_favorites_count,
    firebase_aggregated_users.user_bookings_count
from selected_users
left join
    firebase_aggregated_users
    on selected_users.user_id = firebase_aggregated_users.user_id
