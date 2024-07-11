{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

WITH displays AS (
SELECT
    module_id
    , event_date
    , app_version
    , entry_id
    , user_id
    , COALESCE(user_role, "Grand Public") AS user_role
    , unique_session_id
FROM {{ref('int_firebase__native_video_event')}} video_events
LEFT JOIN  {{ ref('int_applicative__user') }} AS u USING(user_id)
WHERE event_name = 'ModuleDisplayedOnHomePage'
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
)

,video_block_redirections AS (
SELECT
    module_id
    , unique_session_id
    , COUNT(DISTINCT entry_id) AS total_homes_consulted
FROM {{ref('int_firebase__native_video_event')}} video_events
WHERE event_name = 'ConsultHome'
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
GROUP BY
    module_id
    , unique_session_id
)

,video_perf_per_user_and_video AS (
SELECT
    module_id
    , video_id
    , entry_id
    , unique_session_id
    , COUNT(DISTINCT CASE WHEN event_name = 'ConsultOffer' THEN offer_id ELSE NULL END) AS offers_consulted
    , COUNT(CASE WHEN event_name = 'HasSeenAllVideo' THEN 1 ELSE NULL END) AS seen_all_video
    , MAX(total_video_seen_duration_seconds) AS total_video_seen_duration_seconds
    , MAX(video_duration_seconds) AS video_duration_seconds
    , SAFE_DIVIDE(MAX(total_video_seen_duration_seconds), MAX(video_duration_seconds)) AS pct_video_seen
FROM {{ref('int_firebase__native_video_event')}} video_events
WHERE event_name != 'ModuleDisplayedOnHomePage'
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
GROUP BY
    module_id
    , video_id
    , entry_id
    , unique_session_id
)

SELECT
    module_id
    , event_date
    , app_version
    , entry_id
    , user_id
    , user_role
    , unique_session_id
    , total_homes_consulted
    , COUNT(DISTINCT video_id) AS total_videos_seen
    , SUM(offers_consulted) AS offers_consulted
    , COUNT(DISTINCT CASE WHEN seen_all_video >0 THEN video_id ELSE NULL END) AS total_videos_all_seen
    , SUM(total_video_seen_duration_seconds) AS total_video_seen_duration_seconds
    , SUM(video_duration_seconds) AS total_video_duration_seconds
    , MAX(pct_video_seen) AS pct_video_seen
FROM displays
LEFT JOIN video_perf_per_user_and_video USING(module_id, entry_id, unique_session_id)
LEFT JOIN video_block_redirections USING(module_id, unique_session_id)
GROUP BY
    module_id
    , event_date
    , app_version
    , entry_id
    , user_id
    , user_role
    , unique_session_id
    , total_homes_consulted
