{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

SELECT
    event_date
    , module_id
    , user_role
    , entry_id
    , app_version
    , COUNT(DISTINCT unique_session_id) AS total_session_display
    , COUNT(DISTINCT CASE WHEN offers_consulted > 0 THEN unique_session_id ELSE NULL END) AS total_session_consult_offer
    , SUM(offers_consulted) AS total_offers_consulted
    , SUM(video_seen_duration_seconds) AS total_video_seen_duration_seconds
    , COUNT(DISTINCT CASE WHEN pct_video_seen >= 0.25 THEN unique_session_id ELSE NULL END) AS total_seen_25_pct_video
    , COUNT(DISTINCT CASE WHEN pct_video_seen >= 0.5 THEN unique_session_id ELSE NULL END) AS total_seen_50_pct_video
    , COUNT(DISTINCT CASE WHEN pct_video_seen >= 0.75 THEN unique_session_id ELSE NULL END) AS total_seen_75_pct_video
FROM {{ref('int_firebase__native_daily_user_video_module')}}
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
GROUP BY 1,2,3,4,5