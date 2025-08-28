{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_timestamp", "data_type": "timestamp"},
        )
    )
}}

select
    event_timestamp,
    event_type,
    event_name,
    'IOS' as operating_system,
    radio_type,
    app_display_version,
    os_version,
    carrier,
    device_name,
    country,
    trace_info.metric_info.metric_value as home_time_to_interactive_container_in_ms
from {{ source("raw", "firebase_ios_performance") }}
where
    event_type = 'TRACE_METRIC' and event_name = 'home_time_to_interactive_in_ms'
    {% if is_incremental() %}
        and timestamp_trunc(event_timestamp, day) = timestamp(date_sub(date('{{ ds() }}'), interval 2 day))
    {% endif %}

union all

select
    event_timestamp,
    event_type,
    event_name,
    'ANDROID' as operating_system,
    radio_type,
    app_display_version,
    os_version,
    carrier,
    device_name,
    country,
    trace_info.metric_info.metric_value as home_time_to_interactive_container_in_ms
from {{ source("raw", "firebase_android_performance") }}
where
    event_type = 'TRACE_METRIC' and event_name = 'home_time_to_interactive_in_ms'
    {% if is_incremental() %}
        and timestamp_trunc(event_timestamp, day) = timestamp(date_sub(date('{{ ds() }}'), interval 2 day))
    {% endif %}
