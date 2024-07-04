{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

WITH displays AS (
SELECT 
    module_id
    , event_date
    , app_version
    , entry_id
    , user_id
    , COALESCE(user_role, "Grand Public") AS user_role
    , unique_session_id
FROM {{ref('int_firebase_native_video_event')}} video_events 
LEFT JOIN  {{ ref('int_applicative__user') }} AS u USING(user_id)
WHERE event_name = 'ModuleDisplayedOnHomePage'
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
)

,video_perf_per_user_and_video AS (
SELECT 
    module_id
    , video_id
    , entry_id
    , unique_session_id
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id ELSE NULL END) AS offers_consulted
    , MAX(video_seen_duration_seconds) AS video_seen_duration_seconds
    , SAFE_DIVIDE(MAX(video_seen_duration_seconds), MAX(video_duration_seconds)) AS pct_video_seen
FROM {{ref('int_firebase_native_video_event')}} video_events
WHERE event_name != 'ModuleDisplayedOnHomePage'
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
GROUP BY 1,2,3,4
)

SELECT 
    module_id
    , event_date
    , app_version
    , entry_id
    , user_id
    , user_role
    , unique_session_id    
    , COUNT(DISTINCT video_id) AS total_videos_seen
    , SUM(offers_consulted) AS offers_consulted
    , SUM(video_seen_duration_seconds) AS video_seen_duration_seconds
    , MAX(pct_video_seen) AS pct_video_seen
FROM displays
LEFT JOIN video_perf_per_user_and_video USING(module_id, entry_id, unique_session_id)
GROUP BY 1,2,3,4,5,6,7