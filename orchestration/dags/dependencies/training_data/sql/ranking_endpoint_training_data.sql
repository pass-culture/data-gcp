
WITH offer_details AS (
    SELECT
        DISTINCT offer_id,
        is_numerical,
        is_national,
        is_geolocated,
        stock_price,
        category,
        item_id,
        date(offer_creation_date) as offer_creation_date,
        date(stock_beginning_date) as stock_beginning_date,
        subcategory_id,
        booking_number,
        venue_latitude,
        venue_longitude
    FROM
        `{{ bigquery_analytics_dataset }}.recommendable_offers_raw`
),

diversification AS (
    SELECT
        booking_id,
        delta_diversification
    FROM
        `{{ bigquery_analytics_dataset }}.diversification_booking`
),

events AS (
    SELECT
      call_id,
      context,
      date(date) as event_date,	
      pso.origin_offer_id,
      poc.user_id,
      poc.user_bookings_count,
      poc.user_clicks_count,
      poc.user_favorites_count,
      poc.user_deposit_remaining_credit,
      poc.user_iris_id,
      round(poc.offer_user_distance / 1000) as offer_user_distance,
      poc.offer_id,
      poc.offer_item_id,
      poc.offer_booking_number,
      poc.offer_stock_price,
      date_diff(poc.offer_creation_date, event_date, DAY ) as offer_creation_days,
      date_diff(poc.offer_stock_beginning_date, event_date, DAY ) as offer_stock_beginning_days,
      poc.offer_category,
      poc.offer_subcategory_id,
      cast(poc.offer_item_score as FLOAT64) as offer_item_score,
      cast(poc.offer_order as FLOAT64) as offer_order,
      poc.offer_venue_id,
      off.is_numerical,
      off.is_national,
      off.is_geolocated,
      off.venue_latitude,
      off.venue_longitude
    FROM `{{ bigquery_raw_dataset }}.past_offer_context` poc
    INNER JOIN  `{{ bigquery_clean_dataset }}.past_similar_offers` 
      pso on 
      pso.event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY) 
      and pso.event_date = date(poc.date) 
      and pso.reco_call_id = poc.call_id
      and pso.offer_id = poc.offer_id
    INNER JOIN offer_details off on off.offer_id = poc.offer_id
    WHERE offer_order is not null
        
),


seen AS (
    SELECT
        user_id,
        event_date,
        offer_id as origin_offer_id,
        sum(1) as seen
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_similar_offer_events`
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND event_name = 'PlaylistVerticalScroll'
    GROUP BY
        1,
        2,
        3
),
interact AS (
    SELECT
        fsoe.user_id AS user_id,
        fsoe.event_date,
        fsoe.offer_id,
        fsoe.booking_id,
        sum(if(event_name = "ConsultOffer", 1, null)) as consult,
        sum(if(event_name = "BookingConfirmation", 1, null)) as booking,
        sum(d.delta_diversification) as delta_diversification
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_similar_offer_events` fsoe
    LEFT JOIN diversification d on d.booking_id = fsoe.booking_id

    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND event_name in ("ConsultOffer", "BookingConfirmation")
    GROUP BY
        1,
        2,
        3,
        4
),
transactions AS (
    SELECT
        e.*,
        i.booking_id,
        coalesce(i.booking, 0) > 0 as booking,
        coalesce(i.booking, i.consult, 0 ) > 0 as consult,
        coalesce(i.booking, i.consult, s.seen, 0) > 0 as seen,
        coalesce(i.delta_diversification, 0) as delta_diversification
    FROM
        events e
    LEFT JOIN seen s ON s.event_date = e.event_date
        AND s.user_id = e.user_id
        AND s.origin_offer_id = e.origin_offer_id
    LEFT JOIN interact i ON i.event_date = e.event_date
        AND i.user_id = e.user_id
        AND i.offer_id = e.offer_id 
)

SELECT
    user_bookings_count,
    user_clicks_count,
    user_favorites_count,
    user_deposit_remaining_credit,
    offer_user_distance,
    offer_booking_number,
    offer_stock_price,
    offer_creation_days,
    offer_stock_beginning_days,
    offer_subcategory_id,
    is_numerical,
    is_national,
    is_geolocated,
    venue_latitude,
    venue_longitude,
    avg(offer_item_score) as offer_item_score, -- similarity score
    avg(offer_order) as offer_order, -- ranking score 
    max(booking) as booking,
    max(consult) as consult,
    max(seen) as seen,
    max(delta_diversification) as delta_diversification
FROM
    transactions ul
WHERE seen -- at least seen in some context.
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17