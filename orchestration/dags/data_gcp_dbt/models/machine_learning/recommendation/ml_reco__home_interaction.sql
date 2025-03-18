with filtered_offer_displayed as (
    select
        event_date,
        user_id,
        unique_session_id,
        module_id,
        offer_id,
        displayed_position,
        event_timestamp
    from {{ ref("int_firebase__native_home_offer_displayed") }}
    where
        event_date between
            date_sub(date("{{ ds() }}"), interval 28 day)
            and date("{{ ds() }}")
        and module_id is not null
        and user_id is not null
        and unique_session_id is not null
        and offer_id is not null
),

filtered_home_module as (
    select
        module_id,
        user_id,
        unique_session_id,
        module_name,
        module_type,
        offer_id as offer_id_clicked,
        booking_id,
        click_type,
        reco_call_id,
        user_location_type,
        module_clicked_timestamp,
        consult_offer_timestamp,
        module_displayed_timestamp,
        fav_timestamp
    from {{ ref("int_firebase__native_daily_user_home_module") }}
    where
        module_displayed_date between
            date_sub(date("{{ ds() }}"), interval 28 day)
            and date("{{ ds() }}")
        and module_id is not null
        and user_id is not null
        and unique_session_id is not null
        {% if var("ENV_SHORT_NAME") == "prod" %}
        and
            offer_id is not null
            fav_timestamp is not null
            booking_id is not null
        {% endif %}
)

select distinct
    filtered_offer_displayed.event_date,
    filtered_offer_displayed.user_id,
    filtered_offer_displayed.unique_session_id,
    filtered_offer_displayed.module_id,
    filtered_home_module.module_name,
    filtered_home_module.module_type,
    filtered_offer_displayed.offer_id,
    filtered_offer_displayed.displayed_position,
    filtered_home_module.offer_id_clicked,
    filtered_home_module.booking_id,
    filtered_home_module.click_type,
    filtered_home_module.reco_call_id,
    filtered_home_module.user_location_type,
    filtered_home_module.module_clicked_timestamp,
    filtered_home_module.consult_offer_timestamp,
    filtered_offer_displayed.event_timestamp as offer_displayed_timestamp,
    filtered_home_module.offer_id_clicked is not null as is_consulted,
    filtered_home_module.fav_timestamp is not null as is_added_to_favorite,
    filtered_home_module.booking_id is not null as is_booked,
    format_date("%A", filtered_offer_displayed.event_timestamp) as day_of_week,
    extract(hour from filtered_offer_displayed.event_timestamp) as hour_of_day
from filtered_offer_displayed
inner join filtered_home_module
    on filtered_offer_displayed.module_id = filtered_home_module.module_id
    and filtered_offer_displayed.user_id = filtered_home_module.user_id
    and filtered_offer_displayed.unique_session_id = filtered_home_module.unique_session_id
    and filtered_offer_displayed.event_timestamp = filtered_home_module.module_displayed_timestamp
