{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

select
    unique_session_id,
    user_id,
    event_timestamp,
    event_date,
    app_version,
    event_name,
    module_id,
    video_id,
    entry_id,
    ne.offer_id,
    COALESCE(CAST(video_seen_duration_seconds as FLOAT64), 0) total_video_seen_duration_seconds,
    COALESCE(CAST(video_duration_seconds as FLOAT64), 0) video_duration_seconds
from {{ ref('int_firebase__native_event') }} ne
    inner join {{ ref('int_contentful__entry' ) }} ce on ne.module_id = ce.id
        and ce.content_type in ('video', 'videoCarousel', 'videoCarouselItem')
where event_name in ('ConsultVideo', 'HasSeenAllVideo', 'HasDismissedModal', 'VideoPaused', 'ModuleDisplayedOnHomePage', 'ConsultOffer', 'ConsultHome')
    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE("{{ ds() }}"), interval 1 day) and DATE("{{ ds() }}")
    {% endif %}
