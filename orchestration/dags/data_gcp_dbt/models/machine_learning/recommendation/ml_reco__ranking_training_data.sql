{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

with
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
            interaction_is_geolocated
        from {{ ref("ml_reco__home_interaction") }}
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
            offer_iris_internal_id,
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
            booking_number_last_7_days,
            avg_semantic_embedding as semantic_emb_mean
        from {{ ref("ml_feat__item_feature_28_day") }}
    ),

    user_daily_iris_location as (
        select
            user_id,
            event_date,
            user_iris_id,
            user_centroid,
            user_centroid_x,
            user_centroid_y
        from {{ ref("ml_feat__user_daily_iris_location") }}
    ),

    user_features as (
        select
            user_id,
            user_clicks_count,
            user_favorites_count,
            user_bookings_count,
            user_deposit_initial_amount,
            user_theoretical_remaining_credit
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
    offer_features.offer_iris_internal_id,
    offer_features.offer_centroid,
    offer_features.offer_centroid_x,
    offer_features.offer_centroid_y,
    item_features_28_day.booking_number_last_28_days
    as item_booking_number_last_28_days,
    item_features_28_day.booking_number_last_14_days
    as item_booking_number_last_14_days,
    item_features_28_day.booking_number_last_7_days as item_booking_number_last_7_days,
    item_features_28_day.semantic_emb_mean,
    user_daily_iris_location.user_iris_id,
    user_daily_iris_location.user_centroid,
    user_daily_iris_location.user_centroid_x,
    user_daily_iris_location.user_centroid_y,
    user_features.user_bookings_count,
    user_features.user_clicks_count,
    user_features.user_favorites_count,
    user_features.user_deposit_initial_amount,
    user_features.user_theoretical_remaining_credit,
    to_json_string(item_embeddings.item_embedding) as item_embedding_json,
    to_json_string(user_embeddings.user_embedding) as user_embedding_json,
    (
        select sum(x * y) as dot_product
        from unnest(item_embeddings.item_embedding) as x
        with
        offset as pos
        inner join unnest(user_embeddings.user_embedding) as y
        with
        offset as pos2 on pos = pos2  -- noqa: RF01, RF02
    ) as item_user_similarity,
    st_distance(
        offer_features.offer_centroid, user_daily_iris_location.user_centroid
    ) as offer_user_distance

from home_interactions
left join offer_features on home_interactions.offer_id = offer_features.offer_id
left join item_embeddings on offer_features.item_id = item_embeddings.item_id
left join user_embeddings on home_interactions.user_id = user_embeddings.user_id
left join item_features_28_day on offer_features.item_id = item_features_28_day.item_id
left join
    user_daily_iris_location
    on home_interactions.user_id = user_daily_iris_location.user_id
    and home_interactions.event_date = user_daily_iris_location.event_date
left join user_features on home_interactions.user_id = user_features.user_id
order by
    home_interactions.event_date,
    home_interactions.user_id,
    home_interactions.unique_session_id,
    home_interactions.module_id,
    home_interactions.displayed_position
