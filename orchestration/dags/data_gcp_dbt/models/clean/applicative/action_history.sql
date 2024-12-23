select
    *
    ,REPLACE(CAST(JSON_EXTRACT_SCALAR(action_history_json_data, '$.modified_info.postalCode.old_info') AS STRING), '"', '') AS old_postal_code
    ,REPLACE(CAST(JSON_EXTRACT_SCALAR(action_history_json_data, '$.modified_info.postalCode.new_info') AS STRING), '"', '') AS new_postal_code
    ,REPLACE(CAST(JSON_EXTRACT_SCALAR(action_history_json_data, '$.modified_info.activity.old_info') AS STRING), '"', '') AS old_activity
    ,REPLACE(CAST(JSON_EXTRACT_SCALAR(action_history_json_data, '$.modified_info.activity.new_info') AS STRING), '"', '') AS new_activity
from {{ source("raw", "applicative_database_action_history") }}
