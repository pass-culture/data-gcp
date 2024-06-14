WITH diversification AS (
    SELECT
        im.item_id,
        avg(d.delta_diversification) as delta_diversification
    FROM {{ ref("diversification_booking") }} d
    INNER JOIN {{ ref('offer_item_ids') }} AS im on d.offer_id = im.offer_id
    WHERE
        date(booking_creation_date) > DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
    GROUP BY
        im.item_id
),

events AS (
    SELECT
        event_date,
        reco_call_id,
        user_id,
        offer_id,
        item_id,
        user_context.user_deposit_remaining_credit,
        user_context.user_bookings_count,
        user_context.user_clicks_count,
        user_context.user_favorites_count,
        user_context.user_is_geolocated,
        ST_X(user_context.user_iris_centroid) as user_iris_x,
        ST_Y(user_context.user_iris_centroid) as user_iris_y,
        context as context,
        offer_context.offer_item_rank,
        offer_context.offer_user_distance,
        offer_context.offer_is_geolocated,
        offer_context.offer_stock_price,
        offer_context.offer_category,
        offer_context.offer_subcategory_id,
        offer_context.offer_item_score,
        offer_context.offer_booking_number_last_7_days,
        offer_context.offer_booking_number_last_14_days,
        offer_context.offer_booking_number_last_28_days,
        offer_context.offer_semantic_emb_mean,
        MOD(EXTRACT(DAYOFWEEK FROM event_date) + 5, 7) + 1 AS day_of_the_week,
        EXTRACT(HOUR FROM event_created_at) AS hour_of_the_day,
        date_diff(offer_context.offer_creation_date, event_date, DAY) as offer_creation_days,
        date_diff(offer_context.offer_stock_beginning_date, event_date, DAY) as offer_stock_beginning_days,
        offer_display_order
    FROM
        {{ ref('int_pcreco__displayed_offer_event') }}  poc
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND user_id != "-1" 
        AND offer_display_order <= 30
        AND (total_module_consult_offer + total_module_booking_confirmation + total_module_add_to_favorites) > 0
 ),
interact AS (
    SELECT
        fsoe.user_id,
        im.item_id,
        sum(is_consult_offer) as consult,
        sum(is_add_to_favorites + is_booking_confirmation) as booking,
        max(d.delta_diversification) as delta_diversification
    FROM
        {{ ref('int_firebase__native_event') }} fsoe
    INNER JOIN {{ ref('offer_item_ids') }} AS im on fsoe.offer_id = im.offer_id
    LEFT JOIN diversification d on d.item_id = im.item_id
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND event_name in (
            "ConsultOffer", 
            "BookingConfirmation", 
            "HasAddedOfferToFavorites"
        )
    GROUP BY
        user_id,
        item_id
),

transactions AS (
    SELECT
        e. *,
        coalesce(i.booking, 0) > 0 as booking,
        (coalesce(i.booking, 0) + coalesce(i.consult, 0)) > 0 as consult,
        coalesce(i.delta_diversification, 0) as delta_diversification -- get all past events
    FROM events e 
    LEFT JOIN interact i ON i.user_id = e.user_id AND i.item_id = e.item_id
)

SELECT

    user_deposit_remaining_credit,
    user_bookings_count,
    user_clicks_count,
    user_favorites_count,
    user_is_geolocated,
    user_iris_x,
    user_iris_y,
    context,
    offer_item_rank,
    offer_user_distance,
    offer_is_geolocated,
    offer_stock_price,
    offer_category,
    offer_subcategory_id,
    offer_item_score,
    offer_booking_number_last_7_days,
    offer_booking_number_last_14_days,
    offer_booking_number_last_28_days,
    offer_semantic_emb_mean,
    day_of_the_week,
    hour_of_the_day,
    offer_creation_days,
    offer_stock_beginning_days,
    avg(offer_display_order) as offer_order,
    max(booking) as booking,
    max(consult) as consult,
    max(delta_diversification) as delta_diversification
FROM
    transactions ul
GROUP BY
    user_deposit_remaining_credit,
    user_bookings_count,
    user_clicks_count,
    user_favorites_count,
    user_is_geolocated,
    user_iris_x,
    user_iris_y,
    context,
    offer_item_rank,
    offer_user_distance,
    offer_is_geolocated,
    offer_stock_price,
    offer_category,
    offer_subcategory_id,
    offer_item_score,
    offer_booking_number_last_7_days,
    offer_booking_number_last_14_days,
    offer_booking_number_last_28_days,
    offer_semantic_emb_mean,
    day_of_the_week,
    hour_of_the_day,
    offer_creation_days,
    offer_stock_beginning_days
   