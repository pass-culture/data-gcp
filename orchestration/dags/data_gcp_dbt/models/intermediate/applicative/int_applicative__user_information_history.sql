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

WITH temp AS (
SELECT
    user_id,
    action_date AS user_information_modified_at,
    date(action_date) AS event_date,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.postalCode.new_info'
            ) AS string
        ),
        '"',
        ''
    ) AS user_postal_code,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.activity.new_info'
            ) AS string
        ),
        '"',
        ''
    ) AS user_activity
FROM {{ source("raw", "applicative_database_action_history") }}
WHERE
    action_type = 'INFO_MODIFIED'
    {% if is_incremental() %}
        and date(action_date) >= date_sub('{{ ds() }}', interval 1 day)
    {% else %}
        AND date(action_date)
        >= date_sub('{{ ds() }}', INTERVAL {{ var("full_refresh_lookback") }})
    {% endif %}
)

SELECT
    user_id,
    user_information_modified_at,
    event_date,
    user_postal_code,
    user_activity
FROM temp
WHERE (user_activity IS NOT null OR user_postal_code IS NOT null)
