-- todo : update Métabase avec user_supension
-- update toutes les références dans le repo
select
    action_history_id,
    action_history_json_data,
    action_type,
    action_date,
    author_user_id,
    author_email,
    blacklisted_domain,
    user_id,
    offerer_id,
    venue_id,
    comment,
    JSON_EXTRACT_SCALAR(action_history_json_data, "$.reason") as action_history_reason,
    ROW_NUMBER() over (
        partition by user_id
        order by
            CAST(action_history_id as INTEGER) desc
    ) as action_history_rk
from {{ source('raw', 'applicative_database_action_history') }}
where action_type in ('USER_SUSPENDED', 'USER_UNSUSPENDED')
