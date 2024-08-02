with diversification as (
    select
        im.item_id,
        avg(d.delta_diversification) as delta_diversification
    from {{ ref("diversification_booking") }} d
        inner join {{ ref('offer_item_ids') }} as im on d.offer_id = im.offer_id
    where
        date(booking_creation_date) > date_sub(current_date, interval 90 day)
    group by
        im.item_id
),

interaction AS (
    SELECT
        event_date,
        reco_call_id,
        max(if(is_consult_offer + is_booking_confirmation + is_add_to_favorites > 0, offer_display_order, -1)) as last_offer_display_order,
    from
        {{ ref('int_pcreco__displayed_offer_event') }} poc
    where
        event_date >= date_sub(current_date, interval 14 day)
        and user_id != "-1"
        and (total_module_consult_offer + total_module_booking_confirmation + total_module_add_to_favorites) > 0
    GROUP BY event_date, reco_call_id
),

events as (
    select
        poc.event_date,
        poc.reco_call_id,
        poc.user_id,
        poc.offer_id,
        item_id,
        user_context.user_deposit_remaining_credit,
        user_context.user_bookings_count,
        user_context.user_clicks_count,
        user_context.user_favorites_count,
        user_context.user_is_geolocated,
        st_x(user_context.user_iris_centroid) as user_iris_x,
        st_y(user_context.user_iris_centroid) as user_iris_y,
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
        mod(extract(dayofweek from poc.event_date) + 5, 7) + 1 as day_of_the_week,
        extract(hour from event_created_at) as hour_of_the_day,
        date_diff(offer_context.offer_creation_date, poc.event_date, day) as offer_creation_days,
        date_diff(offer_context.offer_stock_beginning_date, poc.event_date, day) as offer_stock_beginning_days,
        offer_display_order
    from
        {{ ref('int_pcreco__displayed_offer_event') }} poc
    inner join interaction ii on ii.event_date = poc.event_date 
        and ii.reco_call_id = poc.reco_call_id and poc.offer_display_order <= ii.last_offer_display_order
    where
        poc.event_date >= date_sub(current_date, interval 14 day)
),

interact as (
    select
        fsoe.user_id,
        im.item_id,
        sum(is_consult_offer) as consult,
        sum(is_add_to_favorites + is_booking_confirmation) as booking
    from
        {{ ref('int_firebase__native_event') }} fsoe
        inner join {{ ref('offer_item_ids') }} as im on fsoe.offer_id = im.offer_id
    where
        event_date >= date_sub(current_date, interval 14 day)
        and event_name in (
            "ConsultOffer",
            "BookingConfirmation",
            "HasAddedOfferToFavorites"
        )
    group by
        user_id,
        item_id
),

transactions as (
    select
        e.*,
        coalesce(i.booking, 0) > 0 as booking,
        (coalesce(i.booking, 0) + coalesce(i.consult, 0)) > 0 as consult,
        coalesce(d.delta_diversification, 0) as delta_diversification
    from events e
        left join interact i on i.user_id = e.user_id and i.item_id = e.item_id
        left join diversification d on d.item_id = e.item_id
)

select

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
from
    transactions ul
group by
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
