SELECT
    *
    , JSON_EXTRACT_SCALAR(action_history_json_data, "$.reason") AS action_history_reason
    , ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY
            CAST(action_history_id AS INTEGER) DESC
    ) AS rank
FROM {{ source('raw', 'applicative_database_action_history') }}
WHERE action_type IN ('USER_SUSPENDED', 'USER_UNSUSPENDED')
AND rank = 1