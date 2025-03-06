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
    module_id,
    module_name,
    event_date,
    app_version,
    entry_id,
    user_id,
    user_role,
    unique_session_id,
    content_type,
    coalesce(sum(total_homes_consulted), 0) as total_homes_consulted,
    coalesce(count(distinct video_id), 0) as total_videos_seen,
    case when count(distinct video_id) > 1 then true else false end as has_swiped,
    coalesce(sum(offers_consulted), 0) as offers_consulted,
    coalesce(
        count(
            distinct case
                when
                    total_video_seen_duration_seconds = total_video_duration_seconds
                    and total_video_duration_seconds <> 0.0
                then video_id
                else null
            end
        ),
        0
    ) as total_videos_all_seen,
    coalesce(
        sum(total_video_seen_duration_seconds), 0
    ) as total_video_seen_duration_seconds,
    coalesce(sum(total_video_duration_seconds), 0) as total_video_duration_seconds,
    coalesce(max(pct_video_seen), 0) as pct_video_seen,

from {{ ref("int_firebase__native_daily_user_video_conversion") }}
where
    content_type = "videoCarousel"
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} and date(event_date) >= "2024-01-01"
    {% endif %}
group by
    module_id,
    module_name,
    event_date,
    app_version,
    entry_id,
    user_id,
    user_role,
    unique_session_id,
    content_type
