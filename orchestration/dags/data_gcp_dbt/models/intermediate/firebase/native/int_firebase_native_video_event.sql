{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

SELECT 
    unique_session_id
    , user_id
    , event_timestamp
    , event_date
    , app_version
    , event_name
    , module_id
    , video_id
    , entry_id
    , ne.offer_id
    , COALESCE(CAST(video_seen_duration_seconds AS FLOAT64),0) video_seen_duration_seconds -- A nettoyer en raw 
    , COALESCE(CAST(video_duration_seconds AS FLOAT64),0) video_duration_seconds -- A nettoyer en raw
FROM {{ref('int_firebase__native_event')}} ne
INNER JOIN {{ ref('int_contentful__entry' )}}  ce ON ne.module_id = ce.id 
                                            AND ce.content_type IN ('video','videoCarousel', 'videoCarouselItem')
WHERE event_name IN ('ConsultVideo','HasSeenAllVideo', 'HasDismissedModal', 'VideoPaused', 'ModuleDisplayedOnHomePage', 'ConsultOffer')
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}