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

select
    user_id,
    action_date as information_modified_at,
    date(action_date) as event_date,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.postalCode.new_info'
            ) as string
        ),
        '"',
        ''
    ) as user_postal_code,
    replace(
        cast(
            json_extract_scalar(
                action_history_json_data, '$.modified_info.activity.new_info'
            ) as string
        ),
        '"',
        ''
    ) as user_activity
from {{ source("raw", "applicative_database_action_history") }}
where
    action_type = 'INFO_MODIFIED'
    {% if is_incremental() %}
        and date(action_date) >= date_sub('{{ ds() }}', interval 1 day)
    {% else %}
        and date(action_date)
        >= date_sub('{{ ds() }}', interval {{ var("full_refresh_lookback") }})
    {% endif %}
    and (
        replace(
            cast(
                json_extract_scalar(
                    action_history_json_data, '$.modified_info.postalCode.new_info'
                ) as string
            ),
            '"',
            ''
        )
        is not null
        or replace(
            cast(
                json_extract_scalar(
                    action_history_json_data, '$.modified_info.activity.new_info'
                ) as string
            ),
            '"',
            ''
        )
        is not null
    )
