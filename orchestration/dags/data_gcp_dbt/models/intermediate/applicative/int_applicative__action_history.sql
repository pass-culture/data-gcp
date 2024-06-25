-- todo : update Métabase avec user_supension
-- update toutes les références dans le repo
SELECT
    action_history_id,
    action_history_json_data,
    action_type,
    action_date	D,
    author_user_id,
    author_email,
    blacklisted_domain,
    user_id,
    offerer_id,
    venue_id,
    comment,
    JSON_EXTRACT_SCALAR(action_history_json_data, "$.reason") AS action_history_reason,
    ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY
            CAST(action_history_id AS INTEGER) DESC
    ) AS action_history_rk
FROM {{ source('raw', 'applicative_database_action_history') }}
WHERE action_type IN ('USER_SUSPENDED', 'USER_UNSUSPENDED')
