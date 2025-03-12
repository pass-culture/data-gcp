with
    home_displays as (
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
            home_module.user_location_type,
            home_module.module_clicked_timestamp,
            home_module.consult_offer_timestamp,
            offer_displayed.event_timestamp as offer_displayed_timestamp,
            home_module.offer_id is not null as is_consulted,
            home_module.fav_timestamp is not null as is_added_to_favorite,
            home_module.booking_id is not null as is_booked,
            format_date("%A", offer_displayed.event_timestamp) as day_of_week,
            extract(hour from offer_displayed.event_timestamp) as hour_of_day
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
    ),

    home_interactions as (
        select
            event_date,
            user_id,
            unique_session_id,
            module_id,
            module_name,
            module_type,
            offer_id,
            displayed_position,
            offer_id_clicked,
            booking_id,
            click_type,
            reco_call_id,
            user_location_type,
            module_clicked_timestamp,
            consult_offer_timestamp,
            offer_displayed_timestamp,
            is_consulted,
            is_added_to_favorite,
            is_booked,
            day_of_week,
            hour_of_day
        from home_displays
        where is_consulted is true or is_added_to_favorite is true or is_booked is true
    ),

    offers as (
        select
            offer_id,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            offer_url is null as is_geolocated,
            date_diff(
                current_date(), offer_creation_date, day
            ) as offer_created_delta_in_days
        from {{ ref("int_global__offer") }}
    ),

    item_embedding as (
        select
            item_id,
            item_embedding
        from {{ ref("ml_feat__two_tower_last_item_embedding") }}
    ),

    user_embedding as (
        select
            user_id,
            user_embedding
        from {{ ref("ml_feat__two_tower_last_user_embedding") }}
    )

select
    home_interactions.event_date,
    home_interactions.user_id,
    home_interactions.unique_session_id,
    home_interactions.module_id,
    home_interactions.module_name,
    home_interactions.module_type,
    home_interactions.offer_id,
    home_interactions.displayed_position,
    home_interactions.click_type,
    home_interactions.reco_call_id,
    home_interactions.user_location_type,
    home_interactions.module_clicked_timestamp,
    home_interactions.consult_offer_timestamp,
    home_interactions.offer_displayed_timestamp,
    home_interactions.is_consulted,
    home_interactions.is_added_to_favorite,
    home_interactions.is_booked,
    home_interactions.day_of_week,
    home_interactions.hour_of_day,
    offers.item_id,
    offers.offer_category_id,
    offers.offer_subcategory_id,
    offers.is_geolocated,
    offers.offer_created_delta_in_days,
    item_embedding.item_embedding,
    user_embedding.user_embedding

from home_interactions
left join offers on home_interactions.offer_id = offers.offer_id
left join item_embedding on offers.item_id = item_embedding.item_id
left join user_embedding on home_interactions.user_id = user_embedding.user_id
order by event_date, user_id, unique_session_id, module_id, displayed_position
