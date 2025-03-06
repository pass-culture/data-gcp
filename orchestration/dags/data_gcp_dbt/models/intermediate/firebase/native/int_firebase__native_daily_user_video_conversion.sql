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
    displays as (
        select
            module_id,
            content_type,
            module_name,
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
            ce_parent.title as module_name
        from
            {{ ref("int_contentful__entry") }} ce_parent,
            unnest(split(ce_parent.items)) as item_id
        where ce_parent.content_type = 'videoCarousel'
    ),

    consult_reattribution as (
        select
            cfci.event_name,
            cfci.offer_id,
            ctim.parent_module_id as module_id,
            ctim.content_type,
            ctim.module_name,
            cfci.unique_session_id,
            cfci.user_id,
            cfci.event_timestamp,
            cfci.event_date,
            cfci.app_version,
            cfci.video_id,
            cfci.entry_id
        from consult_from_carousel_item cfci
        left join carousel_to_item_map ctim on cfci.module_id = ctim.module_id
    ),

    video_perf_per_user_and_video as (
        select
            coalesce(cr.module_id, ve.module_id) as module_id,
            coalesce(cr.content_type, ve.content_type) as content_type,
            coalesce(cr.module_name, ve.module_name) as module_name,
            ve.video_id,
            ve.entry_id,
            ve.unique_session_id,
            count(
                distinct case
                    when cr.event_name = 'ConsultOffer'
                    then coalesce(cr.offer_id, ve.offer_id)
                    else null
                end
            ) as offers_consulted,
            max(
                ve.total_video_seen_duration_seconds
            ) as total_video_seen_duration_seconds,
            max(ve.video_duration_seconds) as video_duration_seconds,
            safe_divide(
                max(ve.total_video_seen_duration_seconds),
                max(ve.video_duration_seconds)
            ) as pct_video_seen
        from {{ ref("int_firebase__native_video_event") }} ve
        left join
            consult_reattribution cr on ve.unique_session_id = cr.unique_session_id
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
            coalesce(cr.module_id, ve.module_id),
            coalesce(cr.content_type, ve.content_type),
            coalesce(cr.module_name, ve.module_name),
            video_id,
            entry_id,
            unique_session_id
    )

select
    displays.module_id,
    displays.content_type,
    displays.module_name,
    event_date,
    app_version,
    entry_id,
    video_id,
    user_id,
    user_role,
    unique_session_id,
    coalesce(total_homes_consulted, 0) as total_homes_consulted,
    coalesce(sum(offers_consulted), 0) as offers_consulted,
    coalesce(
        sum(total_video_seen_duration_seconds), 0
    ) as total_video_seen_duration_seconds,
    coalesce(sum(video_duration_seconds), 0) as total_video_duration_seconds,
    coalesce(max(pct_video_seen), 0) as pct_video_seen
from displays
left join video_perf_per_user_and_video using (module_id, entry_id, unique_session_id)
left join video_block_redirections using (module_id, unique_session_id)
group by
    displays.module_id,
    displays.content_type,
    displays.module_name,
    event_date,
    app_version,
    entry_id,
    video_id,
    user_id,
    user_role,
    unique_session_id,
    total_homes_consulted
