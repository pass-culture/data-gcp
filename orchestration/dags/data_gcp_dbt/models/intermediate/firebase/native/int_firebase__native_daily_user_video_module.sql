{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

with displays as (
    select
        module_id,
        event_date,
        app_version,
        entry_id,
        user_id,
        COALESCE(user_role, "Grand Public") as user_role,
        unique_session_id
    from {{ ref('int_firebase__native_video_event') }} video_events
        left join {{ ref('int_applicative__user') }} as u using (user_id)
    where
        event_name = 'ModuleDisplayedOnHomePage'
        {% if is_incremental() %}
            and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
        {% endif %}
),

video_block_redirections as (
    select
        module_id,
        unique_session_id,
        COUNT(distinct entry_id) as total_homes_consulted
    from {{ ref('int_firebase__native_video_event') }} video_events
    where
        event_name = 'ConsultHome'
        {% if is_incremental() %}
            and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
        {% endif %}
    group by
        module_id,
        unique_session_id
),

video_perf_per_user_and_video as (
    select
        module_id,
        video_id,
        entry_id,
        unique_session_id,
        COUNT(distinct case when event_name = 'ConsultOffer' then offer_id else NULL end) as offers_consulted,
        COUNT(case when event_name = 'HasSeenAllVideo' then 1 else NULL end) as seen_all_video,
        MAX(total_video_seen_duration_seconds) as total_video_seen_duration_seconds,
        MAX(video_duration_seconds) as video_duration_seconds,
        SAFE_DIVIDE(MAX(total_video_seen_duration_seconds), MAX(video_duration_seconds)) as pct_video_seen
    from {{ ref('int_firebase__native_video_event') }} video_events
    where
        event_name != 'ModuleDisplayedOnHomePage'
        {% if is_incremental() %}
            and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
        {% endif %}
    group by
        module_id,
        video_id,
        entry_id,
        unique_session_id
)

select
    module_id,
    event_date,
    app_version,
    entry_id,
    user_id,
    user_role,
    unique_session_id,
    total_homes_consulted,
    COUNT(distinct video_id) as total_videos_seen,
    SUM(offers_consulted) as offers_consulted,
    COUNT(distinct case when seen_all_video > 0 then video_id else NULL end) as total_videos_all_seen,
    SUM(total_video_seen_duration_seconds) as total_video_seen_duration_seconds,
    SUM(video_duration_seconds) as total_video_duration_seconds,
    MAX(pct_video_seen) as pct_video_seen
from displays
    left join video_perf_per_user_and_video using (module_id, entry_id, unique_session_id)
    left join video_block_redirections using (module_id, unique_session_id)
group by
    module_id,
    event_date,
    app_version,
    entry_id,
    user_id,
    user_role,
    unique_session_id,
    total_homes_consulted
