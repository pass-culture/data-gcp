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
            between date_sub(date("{{ ds() }}"), interval 28 day) and date("{{ ds() }}")
            and home_module.module_displayed_date
            between date_sub(date("{{ ds() }}"), interval 28 day) and date("{{ ds() }}")
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
            module_clicked_timestamp,
            consult_offer_timestamp,
            offer_displayed_timestamp,
            is_consulted,
            is_added_to_favorite,
            is_booked,
            day_of_week,
            hour_of_day,
            coalesce(
                user_location_type = "UserGeolocation", false
            ) as interaction_is_geolocated
        from home_displays
        {% if var("ENV_SHORT_NAME") != "prod" %}
            where
                is_consulted is true
                or is_added_to_favorite is true
                or is_booked is true
        {% endif %}
    ),

    offer_features as (
        select
            offer_id,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            is_geolocated,
            offer_created_delta_in_days,
            offer_mean_stock_price,
            offer_max_stock_beginning_days,
            offer_iris_id,
            offer_centroid,
            offer_centroid_x,
            offer_centroid_y

        from {{ ref("ml_feat__offer_feature") }}

    ),

    item_embeddings as (
        select item_id, item_embedding
        from {{ ref("ml_feat__two_tower_last_item_embedding") }}
    ),

    user_embeddings as (
        select user_id, user_embedding
        from {{ ref("ml_feat__two_tower_last_user_embedding") }}
    ),

    item_features_28_day as (
        select
            item_id,
            booking_number_last_28_days,
            booking_number_last_14_days,
            booking_number_last_7_days
        from {{ ref("ml_feat__item_feature_28_day") }}
    ),

    user_features as (
        select
            user_id,
            event_date,
            active_week,
            user_iris_id,
            user_centroid,
            user_centroid_x,
            user_centroid_y,
            user_bookings_count,
            user_clicks_count,
            user_favorites_count,
            user_diversification_count,
            user_deposit_amount,
            user_amount_spent
        from {{ ref("ml_feat__user_feature") }}
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
    home_interactions.interaction_is_geolocated,
    home_interactions.module_clicked_timestamp,
    home_interactions.consult_offer_timestamp,
    home_interactions.offer_displayed_timestamp,
    home_interactions.is_consulted,
    home_interactions.is_added_to_favorite,
    home_interactions.is_booked,
    home_interactions.day_of_week,
    home_interactions.hour_of_day,
    offer_features.item_id,
    offer_features.offer_category_id,
    offer_features.offer_subcategory_id,
    offer_features.is_geolocated as offer_is_geolocated,
    offer_features.offer_created_delta_in_days,
    offer_features.offer_mean_stock_price,
    offer_features.offer_max_stock_beginning_days,
    offer_features.offer_iris_id,
    offer_features.offer_centroid,
    offer_features.offer_centroid_x,
    offer_features.offer_centroid_y,
    item_embeddings.item_embedding as item_last_embedding,
    item_features_28_day.booking_number_last_28_days
    as item_booking_number_last_28_days,
    item_features_28_day.booking_number_last_14_days
    as item_booking_number_last_14_days,
    item_features_28_day.booking_number_last_7_days as item_booking_number_last_7_days,
    user_embeddings.user_embedding as user_last_embedding,
    user_features.user_iris_id,
    user_features.user_centroid,
    user_features.user_centroid_x,
    user_features.user_centroid_y,
    user_features.user_bookings_count,
    user_features.user_clicks_count,
    user_features.user_favorites_count,
    user_features.user_diversification_count,
    user_features.user_deposit_amount,
    user_features.user_amount_spent,
    st_distance(
        offer_features.offer_centroid, user_features.user_centroid
    ) as offer_user_distance

from home_interactions
left join offer_features on home_interactions.offer_id = offer_features.offer_id
left join item_embeddings on offer_features.item_id = item_embeddings.item_id
left join user_embeddings on home_interactions.user_id = user_embeddings.user_id
left join item_features_28_day on offer_features.item_id = item_features_28_day.item_id
left join
    user_features
    on home_interactions.user_id = user_features.user_id
    and home_interactions.event_date = user_features.event_date
order by
    home_interactions.event_date,
    home_interactions.user_id,
    home_interactions.unique_session_id,
    home_interactions.module_id,
    home_interactions.displayed_position
