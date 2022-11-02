SELECT
    eu.user_id,
    eu.user_deposit_creation_date,
    eu.user_birth_date,
    eu.user_deposit_initial_amount,
    eu.user_theoretical_remaining_credit,
    eu.booking_cnt,
    au.consult_offer,
    au.has_added_offer_to_favorites,
FROM
    `{{ bigquery_analytics_dataset }}.enriched_user_data` eu
    JOIN `{{ bigquery_analytics_dataset }}.firebase_aggregated_users` au on eu.user_id = au.user_id