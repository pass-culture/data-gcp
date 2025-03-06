with
    offer_with_metatadata as (
        select distinct
            offer_displayed.event_date,
            offer_displayed.user_id,
            offer_displayed.unique_session_id,
            offer_displayed.module_id,
            home_module.module_name,
            home_module.module_type,
            offer_displayed.offer_id,
            offer_displayed.displayed_position,
            home_module.offer_id as offer_id_clicked,
            home_module.booking_id,
            home_module.favorite_id,
            home_module.click_type,
            home_module.reco_call_id,
            home_module.user_location_type,
            home_module.module_clicked_timestamp,
            home_module.consult_offer_timestamp,
            home_module.module_displayed_timestamp,
            offer_displayed.event_timestamp as offer_displayed_timestamp
        from {{ ref("int_firebase__native_home_offer_displayed") }} as offer_displayed
        left join
            {{ ref("int_firebase__native_daily_user_home_module") }} as home_module
            on offer_displayed.module_id = home_module.module_id
            and offer_displayed.user_id = home_module.user_id
            and offer_displayed.unique_session_id = home_module.unique_session_id
            and offer_displayed.event_timestamp = home_module.module_displayed_timestamp
        where
            offer_displayed.event_date
            between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
            and home_module.module_displayed_date
            between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
    )

select
    offer_with_metatadata.event_date,
    offer_with_metatadata.user_id,
    offer_with_metatadata.unique_session_id,
    offer_with_metatadata.module_id,
    offer_with_metatadata.module_name,
    offer_with_metatadata.module_type,
    offer_with_metatadata.offer_id,
    offer_with_metatadata.displayed_position,

    offer_with_metatadata.offer_id_clicked,
    offer_with_metatadata.booking_id,
    offer_with_metatadata.favorite_id,

    offer_with_metatadata.click_type,
    offer_with_metatadata.reco_call_id,
    offer_with_metatadata.user_location_type,
    offer_with_metatadata.module_clicked_timestamp,
    offer_with_metatadata.consult_offer_timestamp,
    offer_with_metatadata.offer_displayed_timestamp,
    case
        when offer_with_metatadata.offer_id_clicked is not null
        then offer_with_metatadata.offer_id_clicked = offer_with_metatadata.offer_id
        else false
    end as is_clicked,
    case
        when offer_with_metatadata.booking_id is not null
        then offer_with_metatadata.booking_id = offer_with_metatadata.offer_id
        else false
    end as is_booked,
    case
        when offer_with_metatadata.favorite_id is not null
        then offer_with_metatadata.favorite_id = offer_with_metatadata.offer_id
        else false
    end as is_added_to_favorite
from offer_with_metatadata
order by
    offer_with_metatadata.unique_session_id,
    offer_with_metatadata.offer_displayed_timestamp,
    offer_with_metatadata.module_id,
    offer_with_metatadata.displayed_position
