-- todo : deprecate this model
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "module_displayed_date", "data_type": "date"},
        )
    )
}}

with
    child_home as (
        -- Home qui proviennent d'un category block
        select distinct e1.home_entry_id, e2.title
        from {{ ref("int_contentful__entry") }} e1
        join {{ ref("int_contentful__entry") }} e2 on e1.home_entry_id = e2.id
        where e1.content_type = "categoryBlock"
    ),

    home_ref as (
        select home.id, home.title
        from {{ ref("int_contentful__entry") }} home
        left join child_home on home.id = child_home.home_entry_id
        where
            child_home.home_entry_id is null  -- retirer les homes qui sont spécifiques
            and home.content_type = "homepageNatif"
    ),

    parents_ref as (
        select *
        from {{ ref("int_contentful__entry") }}
        where content_type in ("categoryList", "thematicHighlight")
    ),

    children_ref as (
        select *
        from {{ ref("int_contentful__entry") }}
        where content_type in ("categoryBlock", "thematic_highlight_info")
    ),

    displayed as (
        select
            user_pseudo_id,
            user_id,
            session_id,
            unique_session_id,
            entry_id,
            home_ref.title as entry_name,
            module_id as parent_module_id,
            ref.title as parent_module_name,
            ref.content_type as parent_module_type,
            event_date as module_displayed_date,
            event_timestamp as module_displayed_timestamp,
            events.user_location_type
        from {{ ref("int_firebase__native_event") }} events
        left join {{ ref("int_contentful__entry") }} as ref on events.module_id = ref.id
        join home_ref on events.entry_id = home_ref.id
        where
            event_name = 'ModuleDisplayedOnHomePage'
            and user_pseudo_id is not null
            and session_id is not null
            {% if is_incremental() %}
                -- and recalculate latest day's data + previous
                and date(event_date)
                >= date_sub(date(_dbt_max_partition), interval 3 day)
            {% endif %}
        qualify
            rank() over (
                partition by unique_session_id, module_id order by event_timestamp
            )
            = 1  -- get the first display event
    ),

    clicked as (
        select
            user_pseudo_id,
            user_id,
            session_id,
            unique_session_id,
            entry_id,
            home_ref.title as entry_name,
            event_name as click_type,
            destination_entry_id,
            e.title as destination_entry_name,
            module_list_id,
            parents_ref.title as module_list_name,  -- category list / highlight name
            module_id,
            children_ref.title as module_name,  -- category block / highlight name
            event_timestamp as module_clicked_timestamp,
            events.user_location_type
        from {{ ref("int_firebase__native_event") }} events
        left join parents_ref on events.module_list_id = parents_ref.id
        left join children_ref on events.module_id = children_ref.id
        left join home_ref on events.entry_id = home_ref.id
        left join
            {{ ref("int_contentful__entry") }} as e
            on events.destination_entry_id = e.id
        where
            event_name in (
                "ExclusivityBlockClicked",
                "CategoryBlockClicked",
                "HighlightBlockClicked",
                "BusinessBlockClicked",
                "ConsultVideo"
            )
            -- entry_id param is missing for event HighlightBlockClicked because it is
            -- available in a prior version of the app.
            and user_pseudo_id is not null
            and session_id is not null
            {% if is_incremental() %}
                -- and recalculate latest day's data + previous
                and date(event_date)
                >= date_sub(date(_dbt_max_partition), interval 3 day)
            {% endif %}
        qualify
            rank() over (
                partition by unique_session_id, module_id order by event_timestamp
            )
            = 1  -- get the first click event
    ),

    consultations as (
        with
            relationships as (
                select distinct
                    parent as home_id, child as playlist_id, e.title as playlist_name
                from {{ ref("int_contentful__relationship") }} r
                left join {{ ref("int_contentful__entry") }} e on r.child = e.id
            ),

            offer as (
                select
                    user_pseudo_id,
                    user_id,
                    session_id,
                    unique_session_id,
                    entry_id,
                    module_id,
                    origin,
                    events.offer_id,
                    events.venue_id,
                    event_timestamp as consult_offer_timestamp,
                    events.user_location_type
                from {{ ref("int_firebase__native_event") }} events
                where
                    event_name = 'ConsultOffer'
                    and origin in (
                        "home",
                        "exclusivity",
                        "venue",
                        "video",
                        "videoModal",
                        "highlightOffer"
                    )
                    and user_pseudo_id is not null
                    {% if is_incremental() %}
                        -- and recalculate latest day's data + previous
                        and date(event_date)
                        >= date_sub(date(_dbt_max_partition), interval 3 day)
                    {% endif %}
                qualify
                    rank() over (
                        partition by unique_session_id, offer_id
                        order by event_timestamp desc
                    )
                    = 1  -- get the last consultation
            ),

            venue as (  -- get the module_id for venue playlist
                select
                    user_pseudo_id,
                    user_id,
                    session_id,
                    unique_session_id,
                    entry_id,
                    module_id,
                    offer_id,
                    venue_id,
                    event_timestamp as consult_venue_timestamp,
                    user_location_type
                from {{ ref("int_firebase__native_event") }}
                where
                    event_name = "ConsultVenue"
                    and origin = "home"
                    and user_pseudo_id is not null
                    {% if is_incremental() %}
                        -- and recalculate latest day's data + previous
                        and date(event_date)
                        >= date_sub(date(_dbt_max_partition), interval 3 day)
                    {% endif %}
                qualify
                    rank() over (
                        partition by unique_session_id, venue_id
                        order by event_timestamp desc
                    )
                    = 1  -- get the last consultation
            )

        select
            offer.user_pseudo_id,
            offer.user_id,
            offer.session_id,
            offer.unique_session_id,
            coalesce(offer.entry_id, venue.entry_id) as entry_id,
            home_ref.title as entry_name,
            coalesce(offer.module_id, venue.module_id) as module_id,
            ref.title as module_name,
            ref.content_type,
            offer.origin,
            coalesce(
                offer.user_location_type, venue.user_location_type
            ) as user_location_type,
            offer.offer_id,
            venue.venue_id,
            consult_offer_timestamp,
            consult_venue_timestamp,
            home_id,
            playlist_id,
            playlist_name
        from venue
        full outer join
            offer
            on offer.unique_session_id = venue.unique_session_id
            and offer.venue_id = venue.venue_id
            and offer.consult_offer_timestamp >= venue.consult_venue_timestamp
        left join home_ref on coalesce(offer.entry_id, venue.entry_id) = home_ref.id
        left join
            {{ ref("int_contentful__entry") }} as ref
            on ref.id = coalesce(offer.module_id, venue.module_id)
        join
            relationships  -- inner join to get only known relationships between playlist and homepages.
            on relationships.playlist_id = coalesce(offer.module_id, venue.module_id)
            and relationships.home_id = coalesce(offer.entry_id, venue.entry_id)
    )

select
    displayed.user_pseudo_id,
    displayed.user_id,
    displayed.session_id,
    displayed.unique_session_id,
    displayed.entry_id,  -- first touch
    displayed.entry_name,
    displayed.parent_module_id,  -- Can be category list block, highlight etc / second touch
    displayed.parent_module_name,
    displayed.parent_module_type,
    displayed.user_location_type,
    destination_entry_id,  -- 2nd home id in case of redirection to an home_id
    destination_entry_name,
    click_type,
    clicked.module_id as child_module_id,  -- category block id
    clicked.module_name as child_module_name,
    playlist_id,
    playlist_name,
    origin,
    content_type,
    consultations.offer_id,
    consultations.venue_id,
    booking_id,
    module_displayed_date,
    module_displayed_timestamp,
    module_clicked_timestamp,
    consult_venue_timestamp,
    consult_offer_timestamp,
    booking_timestamp
from displayed
left join
    clicked
    on displayed.unique_session_id = clicked.unique_session_id
    and displayed.entry_id = clicked.entry_id
    and displayed.parent_module_id = coalesce(clicked.module_list_id, clicked.module_id)  -- coalesce pour ne pas exclure les blocs qui ne redirigent pas vers une home
    and displayed.module_displayed_timestamp <= clicked.module_clicked_timestamp
left join
    consultations
    on displayed.unique_session_id = consultations.unique_session_id
    -- coalesce + conditional joining pour ne pas exclure les consultations d'offres
    -- "directes" => performance ?
    and coalesce(clicked.destination_entry_id, displayed.parent_module_id) = case
        when clicked.destination_entry_id is null
        then playlist_id
        else consultations.home_id
    end
    and coalesce(clicked.module_clicked_timestamp, displayed.module_displayed_timestamp)
    <= consultations.consult_offer_timestamp
left join
    {{ ref("firebase_bookings") }} as bookings
    on displayed.unique_session_id = bookings.unique_session_id
    and consultations.offer_id = bookings.offer_id
    and consultations.consult_offer_timestamp <= bookings.booking_timestamp
