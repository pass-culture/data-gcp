select
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
    home_module.user_location_type,
    home_module.module_clicked_timestamp,
    home_module.consult_offer_timestamp
from {{ ref("int_firebase__native_home_offer_displayed") }} as offer_displayed
left join
    {{ ref("int_firebase__native_daily_user_home_module") }} as home_module
    on offer_displayed.module_id = home_module.module_id
    and offer_displayed.user_id = home_module.user_id
    and offer_displayed.unique_session_id = home_module.unique_session_id
where
    offer_displayed.event_date
    between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
    and home_module.module_displayed_date
    between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
order by
    offer_displayed.unique_session_id,
    offer_displayed.module_id,
    offer_displayed.displayed_position
