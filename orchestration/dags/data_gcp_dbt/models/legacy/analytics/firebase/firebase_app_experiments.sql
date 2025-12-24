{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

select distinct
    event_date,
    user_pseudo_id,
    user_id,
    user_properties.key as experiment_name,
    user_properties.value.string_value as experiment_value
from {{ source("raw", "firebase_events") }}, unnest(user_properties) as user_properties
where
    user_properties.key like "%firebase_exp%"
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% endif %}
qualify
    row_number() over (
        partition by user_pseudo_id, user_id, experiment_name order by event_date desc
    )
    = 1
