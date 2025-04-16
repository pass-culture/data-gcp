with
    selected_users as (
        select
            eu.user_id,
            eu.first_deposit_creation_date as user_deposit_creation_date,
            eu.user_birth_date,
            eu.first_deposit_amount as user_deposit_initial_amount,
            eu.last_deposit_amount as user_last_deposit_amount,
            eu.total_theoretical_remaining_credit,
            eu.total_non_cancelled_individual_bookings as booking_cnt
        from {{ ref("mrt_global__user") }} as eu
        where eu.last_deposit_amount is not null  -- Bad quality data (4 rows in stg and prod)
    ),

    user_embeddings as (
        select user_id, to_json_string(user_embedding) as user_embedding_json
        from {{ ref("ml_feat__two_tower_last_user_embedding") }}
    ),

    user_features as (
        select
            user_id,
            user_bookings_count,
            user_clicks_count,
            user_favorites_count,
            user_deposit_amount,
            user_amount_spent
        from {{ ref("ml_feat__user_feature") }}
    )

select
    selected_users.user_id,
    selected_users.user_deposit_creation_date,
    selected_users.user_birth_date,
    selected_users.user_deposit_initial_amount,
    selected_users.booking_cnt,
    au.consult_offer,
    au.has_added_offer_to_favorites,
    user_features.user_bookings_count as new_user_bookings_count,
    user_features.user_clicks_count as new_user_clicks_count,
    user_features.user_favorites_count as new_user_favorites_count,
    user_features.user_deposit_amount as new_user_deposit_amount,
    user_features.user_amount_spent as new_user_amount_spent,
    user_embeddings.user_embedding_json as new_user_embedding_json,
    coalesce(
        selected_users.total_theoretical_remaining_credit,
        selected_users.user_last_deposit_amount
    ) as user_theoretical_remaining_credit
from selected_users
left join
    {{ ref("firebase_aggregated_users") }} as au on selected_users.user_id = au.user_id
left join user_features on selected_users.user_id = user_features.user_id
left join user_embeddings on selected_users.user_id = user_embeddings.user_id
qualify
    row_number() over (
        partition by selected_users.user_id order by selected_users.booking_cnt desc
    )
    = 1
