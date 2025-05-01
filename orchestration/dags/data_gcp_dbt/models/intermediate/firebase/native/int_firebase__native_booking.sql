{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "booking_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
            require_partition_filter=true,
        )
    )
}}

select
    user_id,
    user_pseudo_id,
    session_id,
    unique_session_id,
    offer_id,
    booking_id,
    event_date as booking_date,
    event_timestamp as booking_timestamp,
    platform,
    user_location_type,
    app_version
from {{ ref("int_firebase__native_event") }}
where
    event_name = 'BookingConfirmation'
    {% if is_incremental() %}
        and date(event_date) >= date_sub('{{ ds() }}', interval 3 day)
    {% else %}
        and date(event_date)
        >= date_sub('{{ ds() }}', interval {{ var("full_refresh_lookback") }})
    {% endif %}
qualify row_number() over (partition by booking_id order by event_timestamp) = 1
