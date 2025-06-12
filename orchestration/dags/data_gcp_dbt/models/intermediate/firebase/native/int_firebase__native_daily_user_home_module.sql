{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "module_displayed_date",
                "data_type": "date",
                "granularity": "day",
            },
            cluster_by="entry_id",
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

with
    redirections as (
        select
            destination_entry_id as entry_id,
            module_id as parent_module_id,
            entry_id as parent_entry_id,
            unique_session_id
        from {{ ref("int_firebase__native_event") }}
        where
            event_name in ('CategoryBlockClicked', 'HighlightBlockClicked')
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
    ),

    displayed as (
        select
            events.unique_session_id,
            events.user_id,
            events.app_version,
            events.event_date as module_displayed_date,
            events.event_timestamp as module_displayed_timestamp,
            events.module_id,
            modules.content_type as module_type,
            events.entry_id,
            homes.title as entry_name,
            redirections.parent_module_id,
            redirections.parent_entry_id,
            events.user_location_type,
            coalesce(modules.title, modules.offer_title) as module_name,
            case
                when modules.content_type = 'recommendation' then events.reco_call_id
            end as reco_call_id
        from {{ ref("int_firebase__native_event") }} as events
        left join
            redirections
            on events.unique_session_id = redirections.unique_session_id
            and events.entry_id = redirections.entry_id
        inner join
            {{ ref("int_contentful__entry") }} as modules
            on events.module_id = modules.id
        inner join
            {{ ref("int_contentful__entry") }} as homes on events.entry_id = homes.id
        where
            event_name = 'ModuleDisplayedOnHomePage'
            and events.unique_session_id is not null
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        qualify
            row_number() over (
                partition by events.unique_session_id, events.module_id
                order by event_timestamp
            )
            = 1
    ),

    clicked as (
        select
            unique_session_id,
            entry_id,
            event_name as click_type,
            module_id,
            module_list_id,
            event_timestamp as module_clicked_timestamp
        from {{ ref("int_firebase__native_event") }}
        where
            unique_session_id is not null
            and event_name in (
                'ExclusivityBlockClicked',
                'CategoryBlockClicked',
                'HighlightBlockClicked',
                'BusinessBlockClicked',
                'ConsultVideo',
                'TrendsBlockClicked'
            )
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        qualify
            row_number() over (
                partition by unique_session_id, module_id order by event_timestamp
            )
            = 1
    ),

    consultations_offer as (
        select
            unique_session_id,
            entry_id,
            module_id,
            origin,
            offer_id,
            venue_id,
            event_timestamp as consult_offer_timestamp,
            user_location_type
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = 'ConsultOffer'
            and origin in (
                'home',
                'exclusivity',
                'venue',
                'video',
                'videoModal',
                'video_carousel_block',
                'highlightOffer'
            )
            and unique_session_id is not null
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        qualify
            row_number() over (
                partition by unique_session_id, offer_id order by event_timestamp desc
            )
            = 1
    ),

    consultations_venue as (
        select
            unique_session_id,
            entry_id,
            module_id,
            offer_id,
            venue_id,
            event_timestamp as consult_venue_timestamp,
            user_location_type
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = 'ConsultVenue'
            and origin in ('home', 'venueList')
            and unique_session_id is not null
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
        qualify
            row_number() over (
                partition by unique_session_id, venue_id order by event_timestamp desc
            )
            = 1
    ),

    consultations as (
        select
            o.unique_session_id,
            o.origin,
            o.offer_id,
            v.venue_id,
            consult_offer_timestamp,
            consult_venue_timestamp,
            coalesce(o.entry_id, v.entry_id) as entry_id,
            coalesce(o.module_id, v.module_id) as module_id,
            coalesce(o.user_location_type, v.user_location_type) as user_location_type
        from consultations_venue as v
        full outer join
            consultations_offer as o
            on v.unique_session_id = o.unique_session_id
            and v.venue_id = o.venue_id
            and v.consult_venue_timestamp <= o.consult_offer_timestamp
    ),

    favorites as (
        select
            consultations.unique_session_id,
            consultations.module_id,
            consultations.entry_id,
            consultations.offer_id,
            events.event_timestamp as fav_timestamp
        from {{ ref("int_firebase__native_event") }} as events
        inner join consultations using (unique_session_id, module_id, offer_id)
        where
            events.event_name = 'HasAddedOfferToFavorites'
            and events.origin in (
                'home',
                'exclusivity',
                'venue',
                'video',
                'videoModal',
                'video_carousel_block',
                'highlightOffer'
            )
            and events.unique_session_id is not null
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
            and consult_offer_timestamp <= events.event_timestamp
        qualify
            row_number() over (
                partition by unique_session_id, offer_id order by event_timestamp desc
            )
            = 1
    )

select
    displayed.user_id,
    displayed.unique_session_id,
    displayed.entry_id,
    displayed.entry_name,
    displayed.module_id,
    displayed.module_name,
    displayed.parent_module_id,
    displayed.parent_entry_id,
    displayed.module_type,
    displayed.user_location_type,
    displayed.reco_call_id,
    displayed.app_version,
    click_type,
    consultations.offer_id,
    consultations.venue_id,
    bookings.booking_id,
    module_displayed_date,
    module_displayed_timestamp,
    module_clicked_timestamp,
    consult_venue_timestamp,
    consult_offer_timestamp,
    fav_timestamp,
    bookings.booking_timestamp
from displayed
left join
    clicked
    on displayed.unique_session_id = clicked.unique_session_id
    and displayed.entry_id = clicked.entry_id
    and (
        displayed.module_id = clicked.module_id
        or displayed.module_id = clicked.module_list_id
    )
    and displayed.module_displayed_timestamp <= clicked.module_clicked_timestamp
left join
    consultations
    on displayed.unique_session_id = consultations.unique_session_id
    and displayed.module_id = consultations.module_id
    and consultations.consult_offer_timestamp >= module_displayed_timestamp
left join
    favorites
    on displayed.unique_session_id = favorites.unique_session_id
    and displayed.module_id = favorites.module_id
left join
    {{ ref("firebase_bookings") }} as bookings
    on displayed.unique_session_id = bookings.unique_session_id
    and consultations.offer_id = bookings.offer_id
    and consultations.consult_offer_timestamp <= bookings.booking_timestamp
