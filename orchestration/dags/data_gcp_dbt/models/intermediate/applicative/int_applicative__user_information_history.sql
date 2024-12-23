{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
            require_partition_filter=false,
        )
    )
}}

SELECT
    user_id,
    action_date AS information_modified_at,
    DATE(action_date) AS event_date,
    REPLACE(
        CAST(
            JSON_EXTRACT_SCALAR(
                action_history_json_data, '$.modified_info.postalCode.new_info'
            ) AS string
        ),
        '"',
        ''
    ) AS user_postal_code,
    REPLACE(
        CAST(
            JSON_EXTRACT_SCALAR(
                action_history_json_data, '$.modified_info.activity.new_info'
            ) AS string
        ),
        '"',
        ''
    ) AS user_activity
FROM {{ source("raw", "applicative_database_action_history") }}
WHERE action_type = 'INFO_MODIFIED'
    {% if is_incremental() %}
        and date(action_date) >= date_sub('{{ ds() }}', interval 1 day)
    {% else %}
        AND DATE(action_date)
        >= DATE_SUB('{{ ds() }}', INTERVAL {{ var("full_refresh_lookback") }})
    {% endif %}
AND (
  REPLACE(
        CAST(
            JSON_EXTRACT_SCALAR(
                action_history_json_data, '$.modified_info.postalCode.new_info'
            ) AS string
        ),
        '"',
        ''
    )  IS NOT null
    OR
REPLACE(
        CAST(
            JSON_EXTRACT_SCALAR(
                action_history_json_data, '$.modified_info.activity.new_info'
            ) AS string
        ),
        '"',
        ''
    ) IS NOT null
)
