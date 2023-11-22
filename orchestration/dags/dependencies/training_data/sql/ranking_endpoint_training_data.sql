WITH diversification AS (
    SELECT
        offer_id,
        avg(delta_diversification) as delta_diversification
    FROM
        `{{ bigquery_analytics_dataset }}.diversification_booking`
    WHERE
        date(booking_creation_date) > DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
    GROUP BY
        1
),
events AS (
    SELECT
        poc.context,
        poc.reco_call_id,
        poc.event_date,
        poc.user_id,
        poc.user_bookings_count,
        poc.user_clicks_count,
        poc.user_favorites_count,
        poc.user_deposit_remaining_credit,
        poc.user_iris_id,
        ST_X(poc.user_iris_centroid) as user_iris_x,
        ST_Y(poc.user_iris_centroid) as user_iris_y,
        poc.user_is_geolocated,
        poc.offer_user_distance as offer_user_distance,
        poc.offer_id,
        poc.offer_item_id,
        poc.offer_is_geolocated,
        poc.offer_booking_number,
        poc.offer_stock_price,
        date_diff(poc.offer_creation_date, event_date, DAY) as offer_creation_days,
        date_diff(poc.offer_stock_beginning_date, event_date, DAY) as offer_stock_beginning_days,
        poc.offer_category,
        poc.offer_subcategory_id,
        cast(poc.offer_item_score as FLOAT64) as offer_item_score,
        cast(poc.offer_order as FLOAT64) as offer_order_score,
        ROW_NUMBER() OVER (
            PARTITION BY reco_call_id,
            user_id
            ORDER BY
                offer_order DESC
        ) as offer_rank
    FROM
        `{{ bigquery_clean_dataset }}.past_offer_context` poc
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY) -- AND context in ('similar_offer', 'recommendation')
        AND user_id != "-1" QUALIFY offer_rank <= 50
),
interact AS (
    SELECT
        fsoe.user_id AS user_id,
        fsoe.offer_id,
        sum(if(event_name = "ConsultOffer", 1, null)) as consult,
        sum(if(event_name = "BookingConfirmation", 1, null)) as booking,
        avg(d.delta_diversification) as delta_diversification
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_events` fsoe
        LEFT JOIN diversification d on d.offer_id = fsoe.offer_id
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND event_name in ("ConsultOffer", "BookingConfirmation")
    GROUP BY
        1,
        2
),
seen AS (
    SELECT
        DISTINCT event_date,
        user_id,
        reco_call_id
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_events` fsoe
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND event_name in ("ConsultOffer", "BookingConfirmation")
),
transactions AS (
    SELECT
        e. *,
        coalesce(i.booking, 0) > 0 as booking,
        coalesce(i.booking, i.consult, 0) > 0 as consult,
        coalesce(i.delta_diversification, 0) as delta_diversification -- get all past events
    FROM
        events e -- where we had one interaction from call_id inapp 
        INNER JOIN seen s ON e.event_date = s.event_date -- and user interact on PL the same day
        AND e.reco_call_id = s.reco_call_id
        AND e.user_id = s.user_id
        LEFT JOIN interact i ON -- offer was consulted or booked 
        i.user_id = e.user_id
        AND i.offer_id = e.offer_id
)
SELECT
    context,
    user_bookings_count,
    user_clicks_count,
    user_favorites_count,
    user_deposit_remaining_credit,
    user_is_geolocated,
    user_iris_x,
    user_iris_y,
    offer_user_distance,
    offer_booking_number,
    offer_stock_price,
    offer_creation_days,
    offer_stock_beginning_days,
    offer_subcategory_id,
    offer_is_geolocated,
    avg(offer_item_score) as offer_item_score,
    -- similarity score
    avg(offer_order_score) as offer_order,
    -- ranking score 
    max(booking) as booking,
    max(consult) as consult,
    max(delta_diversification) as delta_diversification
FROM
    transactions ul
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15