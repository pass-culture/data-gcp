{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        on_schema_change = "sync_all_columns"
    )
) }}

with mapping_module_name_and_id as (
    select *
    from
        (
            select
                relations.parent as module_id,
                entries.title as module_name,
                row_number() over (
                    partition by entries.title
                    order by
                        updated_at desc
                ) as rnk
            from
                {{ ref("int_contentful__relationship") }} relations
                inner join {{ ref("int_contentful__entry") }} entries on entries.id = relations.child
            where
                entries.content_type in (
                    "displayParameters",
                    "venuesSearchParameters",
                    "exclusivityDisplayParameters",
                    "business",
                    "category_bloc",
                    "category_list"
                )
        )
    where
        rnk = 1
),

firebase_events as (
    select
        coalesce(e.module_id, c_name.module_id) as module_id,
        e.*
        except
        (module_id, module_name)
    from {{ ref('int_firebase__native_event') }} e
        left join mapping_module_name_and_id c_name on c_name.module_name = e.module_name
    where
        event_name in (
            'ConsultOffer',
            "ConsultVenue",
            'BusinessBlockClicked',
            'ExclusivityBlockClicked',
            "SeeMoreClicked",
            'ModuleDisplayedOnHomePage',
            "CategoryBlockClicked"
        )
        and (
            origin = 'home'
            or origin = 'exclusivity'
            or origin is NULL
        )
        and event_date >= date_sub('{{ ds() }}', interval 3 day)
        and event_date <= date_add('{{ ds() }}', interval 3 day)
),

firebase_module_events as (
    select
        e.event_date,
        e.event_timestamp,
        -- user
        e.session_id,
        e.user_id,
        e.user_pseudo_id,
        e.unique_session_id,
        e.platform,
        --events
        e.event_name,
        e.offer_id,
        e.booking_id,
        case when entries.content_type = "recommendation" then e.reco_call_id else NULL end as call_id,
        -- modules
        entries.title as module_name,
        entries.content_type,
        e.module_id,
        -- take last seen home_id
        coalesce(
            e.entry_id,
            last_value(e.entry_id ignore nulls) over (
                partition by
                    user_id,
                    session_id
                order by
                    event_timestamp
                range between unbounded preceding
                and current row
            ),
            first_value(e.entry_id ignore nulls) over (
                partition by
                    user_id,
                    session_id
                order by
                    event_timestamp
                range between current row
                and unbounded following
            ),
            last_value(e.entry_id ignore nulls) over (
                partition by module_id
                order by
                    event_date
                range between unbounded preceding
                and current row
            )
        ) as home_id,
        -- take last seen module_id
        coalesce(
            module_index,
            last_value(module_index ignore nulls) over (
                partition by
                    user_id,
                    session_id,
                    module_id
                order by
                    event_timestamp
                range between unbounded preceding
                and current row
            ),
            first_value(module_index ignore nulls) over (
                partition by
                    user_id,
                    session_id,
                    module_id
                order by
                    event_timestamp
                range between current row and unbounded following
            ),
            last_value(module_index ignore nulls) over (
                partition by module_id
                order by
                    event_date
                range between unbounded preceding
                and current row
            )
        ) as module_index
    from
        firebase_events e
        left join {{ ref("int_contentful__entry") }} entries on entries.id = e.module_id
),

-- conversion can be Booking or Favorite
firebase_conversion_step as (
    select
        conv.event_date,
        conv.event_timestamp,
        conv.session_id,
        conv.user_id,
        conv.user_pseudo_id,
        conv.unique_session_id,
        conv.platform,
        --events
        conv.event_name,
        conv.offer_id,
        conv.booking_id,
        event.call_id,
        event.module_name,
        event.content_type,
        event.module_id,
        event.home_id,
        event.module_index,
        -- take last click event
        row_number() over (
            partition by
                conv.session_id,
                conv.user_id,
                conv.offer_id,
                conv.event_name
            order by
                event.event_timestamp desc
        ) as rank
    from {{ ref('int_firebase__native_event') }} conv
        inner join firebase_module_events
            event on event.unique_session_id = conv.unique_session_id
        and event.offer_id = conv.offer_id
        and event.user_id = conv.user_id -- conversion event after click event
    where
        conv.event_name in (
            'BookingConfirmation',
            'HasAddedOfferToFavorites'
        )
        and conv.event_timestamp > event.event_timestamp
        and conv.event_date >= date_sub(date('{{ ds() }}'), interval 3 day)
        and conv.event_date <= date_add(date('{{ ds() }}'), interval 3 day)
),

event_union as (
    select *
    from
        firebase_module_events
    union
    all
    select
        *
        except
        (rank)
    from
        firebase_conversion_step
    where
        rank = 1 -- only one conversion step per event_name
)

select
    e.event_date,
    e.event_timestamp,
    e.session_id,
    e.user_id,
    e.user_pseudo_id,
    e.unique_session_id,
    e.call_id,
    e.platform,
    e.event_name,
    case
        when event_name = "ModuleDisplayedOnHomePage" then "display"
        when event_name in (
            "ConsultOffer",
            "ConsultVenue",
            "BusinessBlockClicked",
            "ExclusivityBlockClicked"
        ) then "click"
        when event_name in ("SeeMoreClicked") then "see_more_click"
        when event_name = "HasAddedOfferToFavorites" then "favorite"
        when event_name = "BookingConfirmation" then "booking"
    end as event_type,
    e.offer_id,
    e.booking_id,
    e.module_name,
    e.content_type,
    e.module_id,
    e.home_id,
    e.module_index
from
    event_union e

{% if is_incremental() %}
-- recalculate latest day's data + previous
    where date(event_date) between date_sub(date('{{ ds() }}'), interval 1 day) and date('{{ ds() }}')
{% endif %}
