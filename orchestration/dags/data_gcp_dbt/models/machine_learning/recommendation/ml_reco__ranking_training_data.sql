with
    home_interactions as (
        select distinct
            offer_displayed.event_date,
            offer_displayed.user_id,
            offer_displayed.unique_session_id,
            offer_displayed.module_id,
            home_module.module_name,
            home_module.module_type as context,
            offer_displayed.offer_id,
            offer_displayed.displayed_position,
            home_module.offer_id as offer_id_clicked,
            home_module.booking_id,
            home_module.click_type,
            home_module.reco_call_id,
            home_module.user_location_type,
            home_module.module_clicked_timestamp,
            home_module.consult_offer_timestamp,
            home_module.module_displayed_timestamp,
            offer_displayed.event_timestamp as offer_displayed_timestamp,
            home_module.offer_id is not null as is_consulted,
            home_module.fav_timestamp is not null as is_added_to_favorite,
            home_module.booking_id is not null as is_booked
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
    home_interactions.event_date,
    home_interactions.user_id,
    home_interactions.unique_session_id,
    home_interactions.module_id,
    home_interactions.module_name,
    home_interactions.module_type as context,
    home_interactions.offer_id,
    home_interactions.displayed_position,
    home_interactions.click_type,
    home_interactions.reco_call_id,
    home_interactions.user_location_type,
    home_interactions.module_clicked_timestamp,
    home_interactions.consult_offer_timestamp,
    home_interactions.offer_displayed_timestamp,
    home_interactions.is_consulted as consult,
    home_interactions.is_added_to_favorite as favorite,
    home_interactions.is_booked as booking,
    format_date("%A", home_interactions.event_date) as day_of_week,
    extract(hour from home_interactions.event_date) as hour_of_day
from home_interactions
