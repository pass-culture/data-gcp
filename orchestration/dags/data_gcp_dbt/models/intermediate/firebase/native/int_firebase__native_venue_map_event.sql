{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

SELECT 
    unique_session_id
    , user_id
    , event_date
    , event_timestamp
    , app_version
    , event_name
    , origin
    , venue_id
    , offer_id
    , SAFE_CAST(duration AS INT64) AS duration_seconds
FROM {{ ref('int_firebase__native_event') }}
WHERE 
(event_name IN ("ConsultVenueMap", "VenueMapSessionDuration", "VenueMapSeenDuration","PinMapPressed")
OR (event_name = 'ConsultVenue' AND origin = "venueMap")
OR (event_name = "ConsultOffer" AND origin IN ("venue", "venuemap", "venueMap") )
)
    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}