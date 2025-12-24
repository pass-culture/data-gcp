{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "booking_date", "data_type": "date"},
            on_schema_change="append_new_columns",
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
    event_name = "BookingConfirmation"
    {% if is_incremental() %}
        -- recalculate latest day's data + previous
        and date(event_date)
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
    {% endif %}
qualify row_number() over (partition by booking_id order by event_timestamp) = 1
