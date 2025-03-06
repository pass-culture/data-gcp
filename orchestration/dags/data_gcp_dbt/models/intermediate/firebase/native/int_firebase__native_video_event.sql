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

with
    all_seen as (
        select unique_session_id, user_id, video_id
        from {{ ref("int_firebase__native_event") }} ne
        join {{ ref("int_contentful__entry") }} ce on ne.module_id = ce.id
        where
            ne.event_date
            between date_sub(date(current_date), interval 1 day) and date(current_date)
            and ne.event_name = "HasSeenAllVideo"
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and date(event_date) >= "2024-01-01"
            {% endif %}
    )

select
    ne.unique_session_id,
    ne.user_id,
    ne.event_timestamp,
    ne.event_date,
    ne.app_version,
    ne.event_name,
    ne.module_id,
    ce.content_type,
    ce.title as module_name,
    ne.video_id,
    ne.entry_id,
    ne.offer_id,
    coalesce(
        case
            when all_seen.video_id is not null and ne.event_name = 'VideoPaused'
            then cast(video_duration_seconds as float64)
            else cast(video_seen_duration_seconds as float64)
        end,
        0
    ) as total_video_seen_duration_seconds,
    coalesce(cast(video_duration_seconds as float64), 0) as video_duration_seconds
from {{ ref("int_firebase__native_event") }} ne
inner join
    {{ ref("int_contentful__entry") }} ce
    on ne.module_id = ce.id
    and ce.content_type in ('video', 'videoCarousel', 'videoCarouselItem')
left join
    all_seen
    on ne.unique_session_id = all_seen.unique_session_id
    and ne.user_id = all_seen.user_id
    and ne.video_id = all_seen.video_id
where
    ne.event_name in (
        'ConsultVideo',
        'HasDismissedModal',
        'VideoPaused',
        'ModuleDisplayedOnHomePage',
        'ConsultOffer',
        'ConsultHome'
    )
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} and date(event_date) >= "2024-01-01"
    {% endif %}
