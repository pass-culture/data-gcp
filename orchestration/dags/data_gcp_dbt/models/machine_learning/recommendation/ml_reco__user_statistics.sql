
WITH selected_users AS (
  SELECT
    eu.user_id,
    eu.user_deposit_creation_date,
    eu.user_birth_date,
    eu.user_deposit_initial_amount,
    eu.user_last_deposit_amount,
    eu.user_theoretical_remaining_credit,
    eu.booking_cnt,
FROM
    {{ ref('enriched_user_data') }} eu
UNION ALL
  SELECT
    ie.user_id,
    null as user_deposit_creation_date,
    u.user_birth_date,
    null as user_deposit_initial_amount,
    null as user_last_deposit_amount,
    null as user_theoretical_remaining_credit,
    0 as booking_cnt,
FROM
    {{ source('raw', 'applicative_database_internal_user') }} ie
LEFT JOIN {{ source('raw', 'applicative_database_user') }} u on u.user_id = ie.user_id
)


SELECT
    eu.user_id,
    eu.user_deposit_creation_date,
    eu.user_birth_date,
    eu.user_deposit_initial_amount,
    coalesce(eu.user_theoretical_remaining_credit, eu.user_last_deposit_amount) as user_theoretical_remaining_credit,
    eu.booking_cnt,
    au.consult_offer,
    au.has_added_offer_to_favorites,
FROM selected_users eu
LEFT JOIN {{ ref('firebase_aggregated_users') }}  au on eu.user_id = au.user_id
QUALIFY ROW_NUMBER() over (PARTITION BY eu.user_id ORDER BY eu.booking_cnt DESC) = 1