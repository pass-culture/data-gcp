select
    *,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.postalCode.old_info'
            ) as string
        ),
        '"',
        ''
    ) as old_postal_code,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.postalCode.new_info'
            ) as string
        ),
        '"',
        ''
    ) as new_postal_code,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.activity.old_info'
            ) as string
        ),
        '"',
        ''
    ) as old_activity,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.activity.new_info'
            ) as string
        ),
        '"',
        ''
    ) as new_activity
from {{ source("raw", "applicative_database_action_history") }}
