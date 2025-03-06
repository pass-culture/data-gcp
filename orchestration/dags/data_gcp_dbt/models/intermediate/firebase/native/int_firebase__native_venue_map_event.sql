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

select
    unique_session_id,
    user_id,
    event_date,
    event_timestamp,
    app_version,
    event_name,
    origin,
    venue_id,
    offer_id,
    safe_cast(duration as int64) as duration_seconds
from {{ ref("int_firebase__native_event") }}
where
    (
        event_name in (
            "ConsultVenueMap",
            "VenueMapSessionDuration",
            "VenueMapSeenDuration",
            "PinMapPressed"
        )
        or (event_name = 'ConsultVenue' and origin = "venueMap")
        or (event_name = "ConsultOffer" and origin in ("venue", "venuemap", "venueMap"))
    )
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% endif %}
