{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns"
    )
) }}

select
    event_date,
    module_id,
    user_role,
    entry_id,
    app_version,
    COUNT(distinct unique_session_id) as total_displayed_sessions,
    COUNT(distinct case when offers_consulted > 0 then unique_session_id else NULL end) as total_sessions_with_consult_offer,
    COUNT(distinct case when total_homes_consulted > 0 then unique_session_id else NULL end) as total_session_consult_home,
    COUNT(distinct case when total_videos_all_seen > 0 then unique_session_id else NULL end) as total_session_seen_all_video,
    SUM(offers_consulted) as total_consulted_offers,
    SUM(total_video_seen_duration_seconds) as total_video_seen_duration_seconds,
    COUNT(distinct case when pct_video_seen >= 0.25 then unique_session_id else NULL end) as total_seen_25_pct_video,
    COUNT(distinct case when pct_video_seen >= 0.5 then unique_session_id else NULL end) as total_seen_50_pct_video,
    COUNT(distinct case when pct_video_seen >= 0.75 then unique_session_id else NULL end) as total_seen_75_pct_video
from {{ ref('int_firebase__native_daily_user_video_module') }}
where
    TRUE
    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}
group by
    event_date,
    module_id,
    user_role,
    entry_id,
    app_version
