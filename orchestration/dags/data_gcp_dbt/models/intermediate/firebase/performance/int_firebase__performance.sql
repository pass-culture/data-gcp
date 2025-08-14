{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_timestamp", "data_type": "timestamp"},
        )
    )
}}

SELECT
    event_timestamp,
    event_type,
    event_name,
    'IOS' AS operating_system,
    radio_type,
    app_display_version,
    os_version,
    carrier,
    device_name,
    country,
    trace_info.metric_info.metric_value AS home_time_to_interactive_container_in_ms
FROM {{ source("raw", "firebase_ios_performance") }}
WHERE event_type = 'TRACE_METRIC'
AND event_name = 'home_time_to_interactive_in_ms'
{% if is_incremental() %}
AND TIMESTAMP_TRUNC(event_timestamp, DAY) = TIMESTAMP(date('{{ ds() }}'))
{% endif %}

UNION ALL

SELECT
    event_timestamp,
    event_type,
    event_name,
    'ANDROID' AS operating_system,
    radio_type,
    app_display_version,
    os_version,
    carrier,
    device_name,
    country,
    trace_info.metric_info.metric_value AS home_time_to_interactive_container_in_ms
FROM {{ source("raw", "firebase_android_performance") }}
WHERE event_type = 'TRACE_METRIC'
AND event_name = 'home_time_to_interactive_in_ms'
{% if is_incremental() %}
AND TIMESTAMP_TRUNC(event_timestamp, DAY) = TIMESTAMP(date('{{ ds() }}'))
{% endif %}
