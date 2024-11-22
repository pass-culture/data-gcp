{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="sync_all_columns",
        )
    )
}}

with
    displays as (
        select
            module_id,
            event_date,
            app_version,
            entry_id,
            user_id,
            coalesce(user_role, "Grand Public") as user_role,
            unique_session_id
        from {{ ref("int_firebase__native_video_event") }} video_events
        left join {{ ref("int_applicative__user") }} as u using (user_id)
        where
            event_name = 'ModuleDisplayedOnHomePage'
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and date(event_date) >= "2024-01-01"
            {% endif %}
    ),

    video_block_redirections as (
        select
            module_id,
            unique_session_id,
            count(distinct entry_id) as total_homes_consulted
        from {{ ref("int_firebase__native_video_event") }} video_events
        where
            event_name = 'ConsultHome'
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and date(event_date) >= "2024-01-01"
            {% endif %}
        group by module_id, unique_session_id
    ),

    consult_from_carousel_item as (
        select
            ne.unique_session_id,
            ne.user_id,
            ne.event_timestamp,
            ne.event_date,
            ne.app_version,
            ne.event_name,
            ne.module_id,
            ne.video_id,
            ne.entry_id,
            ne.offer_id
        from {{ ref("int_firebase__native_event") }} ne
        join {{ ref("int_contentful__entry") }} ce on ne.module_id = ce.id
        where
            ce.content_type = "videoCarouselItem" and ne.event_name = "ConsultOffer"
            {% if is_incremental() %}
                and ne.event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and date(ne.event_date) >= "2024-01-01"
            {% endif %}
    ),

    carousel_to_item_map as (
        select
            ce_parent.id as parent_module_id,
            replace(replace(replace(item_id, '[', ''), ']', ''), "'", '') as module_id,
            ce_parent.content_type as content_type,
        from
            {{ ref("int_contentful__entry") }} ce_parent,
            unnest(split(ce_parent.items)) as item_id
        where ce_parent.content_type = 'videoCarousel'
    ),

    video_perf_per_user_and_video as (
        select
            coalesce(ctim.parent_module_id, ve.module_id) as module_id,
            coalesce(ctim.content_type, ve.content_type) as content_type,
            ve.video_id,
            ve.entry_id,
            ve.unique_session_id,
            count(
                distinct case
                    when ve.event_name = 'ConsultOffer'
                    then coalesce(cfci.offer_id, ve.offer_id)
                    else null
                end
            ) as offers_consulted,
            count(
                case when ve.event_name = 'HasSeenAllVideo' then 1 else null end
            ) as seen_all_video,
            max(
                ve.total_video_seen_duration_seconds
            ) as total_video_seen_duration_seconds,
            max(ve.video_duration_seconds) as video_duration_seconds,
            safe_divide(
                max(ve.total_video_seen_duration_seconds),
                max(ve.video_duration_seconds)
            ) as pct_video_seen
        from {{ ref("int_firebase__native_video_event") }} ve
        left join carousel_to_item_map ctim on ve.module_id = ctim.module_id
        left join
            consult_from_carousel_item cfci
            on ve.unique_session_id = cfci.unique_session_id
            and ve.user_id = cfci.user_id
            and ve.module_id = cfci.module_id
        where
            ve.event_name != 'ModuleDisplayedOnHomePage'
            {% if is_incremental() %}
                and ve.event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and date(ve.event_date) >= "2024-01-01"
            {% endif %}
        group by
            coalesce(ctim.parent_module_id, ve.module_id),
            coalesce(ctim.content_type, ve.content_type),
            video_id,
            entry_id,
            unique_session_id
    )

select
    module_id,
    content_type,
    event_date,
    app_version,
    entry_id,
    video_id,
    user_id,
    user_role,
    unique_session_id,
    coalesce(total_homes_consulted, 0) as total_homes_consulted,
    coalesce(sum(offers_consulted), 0) as offers_consulted,
    coalesce(seen_all_video, 0) as seen_all_video,
    coalesce(
        sum(total_video_seen_duration_seconds), 0
    ) as total_video_seen_duration_seconds,
    coalesce(sum(video_duration_seconds), 0) as total_video_duration_seconds,
    coalesce(max(pct_video_seen), 0) as pct_video_seen
from displays
left join video_perf_per_user_and_video using (module_id, entry_id, unique_session_id)
left join video_block_redirections using (module_id, unique_session_id)
group by
    module_id,
    content_type,
    event_date,
    app_version,
    entry_id,
    video_id,
    user_id,
    user_role,
    unique_session_id,
    total_homes_consulted,
    seen_all_video
