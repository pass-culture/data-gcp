{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

with
    diversification as (
        select im.item_id, avg(b.diversity_score) as delta_diversification
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("int_applicative__offer_item_id") }} as im
            on b.offer_id = im.offer_id
        where
            date(b.booking_creation_date)
            > date_sub(date('{{ ds() }}'), interval 90 day)
        group by im.item_id
    ),

    events as (
        select
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
            context,
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
            offer_display_order,
            st_x(user_context.user_iris_centroid) as user_iris_x,
            st_y(user_context.user_iris_centroid) as user_iris_y,
            mod(extract(dayofweek from event_date) + 5, 7) + 1 as day_of_the_week,
            extract(hour from event_created_at) as hour_of_the_day,
            date_diff(
                offer_context.offer_creation_date, event_date, day
            ) as offer_creation_days,
            date_diff(
                offer_context.offer_stock_beginning_date, event_date, day
            ) as offer_stock_beginning_days
        from {{ ref("int_pcreco__displayed_offer_event") }}
        where
            event_date >= date_sub(
                date('{{ ds() }}'),
                interval {% if var("ENV_SHORT_NAME") == "prod" %} 14
                {% else %} 365
                {% endif %} day
            )  -- 14 days in prod, 1 year in other environments otherwise the data could be empty in ehp
            and user_id != "-1"
            and offer_display_order <= 30
            and (
                total_module_consult_offer
                + total_module_booking_confirmation
                + total_module_add_to_favorites
            )
            > 0
    ),

    interact as (
        select
            fsoe.user_id,
            im.item_id,
            sum(fsoe.is_consult_offer) as consult,
            sum(fsoe.is_add_to_favorites + fsoe.is_booking_confirmation) as booking
        from {{ ref("int_firebase__native_event") }} as fsoe
        inner join
            {{ ref("int_applicative__offer_item_id") }} as im
            on fsoe.offer_id = im.offer_id
        where
            fsoe.event_date >= date_sub(date('{{ ds() }}'), interval 14 day)
            and fsoe.event_name
            in ("ConsultOffer", "BookingConfirmation", "HasAddedOfferToFavorites")
        group by fsoe.user_id, im.item_id
    ),

    transactions as (
        select
            e.*,
            coalesce(i.booking, 0) > 0 as booking,
            (coalesce(i.booking, 0) + coalesce(i.consult, 0)) > 0 as consult,
            coalesce(d.delta_diversification, 0) as delta_diversification
        from events as e
        left join interact as i on e.user_id = i.user_id and e.item_id = i.item_id
        left join diversification as d on e.item_id = d.item_id
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
from transactions
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
