{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns"
    )
) }}

SELECT
    event_date
    , module_id
    , user_role
    , entry_id
    , app_version
    , COUNT(DISTINCT unique_session_id) AS total_displayed_sessions
    , COUNT(DISTINCT CASE WHEN offers_consulted > 0 THEN unique_session_id ELSE NULL END) AS total_sessions_with_consult_offer
    , COUNT(DISTINCT CASE WHEN total_homes_consulted > 0 THEN unique_session_id ELSE NULL END) AS total_session_consult_home
    , COUNT(DISTINCT CASE WHEN total_videos_all_seen > 0 THEN unique_session_id ELSE NULL END) AS total_session_seen_all_video
    , SUM(offers_consulted) AS total_consulted_offers
    , SUM(total_video_seen_duration_seconds) AS total_video_seen_duration_seconds
    , COUNT(DISTINCT CASE WHEN pct_video_seen >= 0.25 THEN unique_session_id ELSE NULL END) AS total_seen_25_pct_video
    , COUNT(DISTINCT CASE WHEN pct_video_seen >= 0.5 THEN unique_session_id ELSE NULL END) AS total_seen_50_pct_video
    , COUNT(DISTINCT CASE WHEN pct_video_seen >= 0.75 THEN unique_session_id ELSE NULL END) AS total_seen_75_pct_video
FROM {{ ref('int_firebase__native_daily_user_video_module') }}
WHERE TRUE
{% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
{% endif %}
GROUP BY
    event_date
    , module_id
    , user_role
    , entry_id
    , app_version
