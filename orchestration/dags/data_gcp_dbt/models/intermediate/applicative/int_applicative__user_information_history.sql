{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=false,
        )
    )
}}

with
    user_attribute_changes as (
        select
            user_id,
            action_date as user_modified_at,
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
    )

select user_id, user_modified_at, event_date, user_postal_code, user_activity
from user_attribute_changes
where (user_activity is not null or user_postal_code is not null)
