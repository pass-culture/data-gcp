with deduplicated_home_offer_displayed as (
    select distinct
    event_date,
    user_id,
    unique_session_id,
    module_id,
    offer_id,
    displayed_position,
    TIMESTAMP_TRUNC(event_timestamp, minute) as event_timestamp_min
from {{ ref("int_firebase__native_home_offer_displayed") }}
)

select
    deduplicated_home_offer_displayed.event_date,
    deduplicated_home_offer_displayed.user_id,
    deduplicated_home_offer_displayed.unique_session_id,
    deduplicated_home_offer_displayed.module_id,
    home_module.module_name,
    home_module.module_type,
    deduplicated_home_offer_displayed.offer_id,
    deduplicated_home_offer_displayed.displayed_position,
    home_module.offer_id as offer_id_clicked,
    home_module.booking_id,
    home_module.click_type,
    home_module.reco_call_id,
    home_module.user_location_type,
    home_module.module_clicked_timestamp,
    home_module.consult_offer_timestamp,
    deduplicated_home_offer_displayed.event_timestamp_min as module_displayed_timestamp
from deduplicated_home_offer_displayed
left join
    {{ ref("int_firebase__native_daily_user_home_module") }} as home_module
    on deduplicated_home_offer_displayed.module_id = home_module.module_id
    and deduplicated_home_offer_displayed.user_id = home_module.user_id
    and deduplicated_home_offer_displayed.unique_session_id = home_module.unique_session_id
where
    deduplicated_home_offer_displayed.event_date
    between DATE_SUB(DATE("{{ ds() }}"), interval 3 day) and DATE("{{ ds() }}")
    and home_module.module_displayed_date
    between DATE_SUB(DATE("{{ ds() }}"), interval 3 day) and DATE("{{ ds() }}")
order by
    deduplicated_home_offer_displayed.unique_session_id,
    deduplicated_home_offer_displayed.module_id,
    deduplicated_home_offer_displayed.displayed_position
