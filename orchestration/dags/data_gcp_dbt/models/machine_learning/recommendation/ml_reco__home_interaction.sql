{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

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
    home_module.click_type,
    home_module.reco_call_id,
    home_module.module_clicked_timestamp,
    home_module.consult_offer_timestamp,
    offer_displayed.event_timestamp as offer_displayed_timestamp,
    home_module.user_location_type = "UserGeolocation" as interaction_is_geolocated,
    coalesce(home_module.offer_id = offer_displayed.offer_id, false) as is_consulted,
    (home_module.offer_id = offer_displayed.offer_id)
    and (home_module.fav_timestamp is not null) as is_added_to_favorite,
    (home_module.offer_id = offer_displayed.offer_id)
    and (home_module.booking_id is not null) as is_booked,
    format_date("%A", offer_displayed.event_timestamp) as day_of_week,
    extract(hour from offer_displayed.event_timestamp) as hour_of_day
from {{ ref("int_firebase__native_home_offer_displayed") }} as offer_displayed
inner join
    {{ ref("int_firebase__native_daily_user_home_module") }} as home_module
    on offer_displayed.module_id = home_module.module_id
    and offer_displayed.user_id = home_module.user_id
    and offer_displayed.unique_session_id = home_module.unique_session_id
    and offer_displayed.event_timestamp = home_module.module_displayed_timestamp
where
    offer_displayed.event_date
    between date_sub(date("{{ ds() }}"), interval 21 day) and date("{{ ds() }}")
    and home_module.module_displayed_date
    between date_sub(date("{{ ds() }}"), interval 21 day) and date("{{ ds() }}")
    {% if var("ENV_SHORT_NAME") == "prod" %}
        and (
            home_module.offer_id is not null
            or home_module.fav_timestamp is not null
            or home_module.booking_id is not null
        )
    {% endif %}
