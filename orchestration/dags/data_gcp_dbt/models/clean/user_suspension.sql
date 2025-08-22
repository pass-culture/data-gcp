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
    action_history_reason,
    action_history_rk
from {{ ref("int_applicative__action_history") }}
