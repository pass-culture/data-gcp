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
    operating_system,
    radio_type,
    app_display_version,
    os_version,
    carrier,
    device_name,
    country,
    home_time_to_interactive_container_in_ms
from {{ ref("int_firebase__performance") }}
where
    true
    {% if is_incremental() %}
        and timestamp_trunc(event_timestamp, day) = timestamp(date_sub(date('{{ ds() }}'), interval 2 day))
    {% endif %}
