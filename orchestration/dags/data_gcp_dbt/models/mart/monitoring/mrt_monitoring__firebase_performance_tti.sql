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
    operating_system,
    radio_type,
    app_display_version,
    os_version,
    carrier,
    device_name,
    country,
    home_time_to_interactive_container_in_ms
FROM {{ ref("int_firebase__performance") }}
WHERE true
{% if is_incremental() %}
AND TIMESTAMP_TRUNC(event_timestamp, DAY) = TIMESTAMP(date('{{ ds() }}'))
{% endif %}
