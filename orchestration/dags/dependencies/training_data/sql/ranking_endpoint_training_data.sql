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
        user_id,
        event_date,
        offer_id,
        origin_offer_id,
        item_rank,
        venue_iris_centroid
    FROM
        `{{ bigquery_clean_dataset }}.past_similar_offers`
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
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
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
        AND event_name = 'PlaylistVerticalScroll'
    GROUP BY
        1,
        2,
        3
),
interact AS (
    SELECT
        user_id AS user_id,
        event_date,
        offer_id,
        booking_id,
        sum(if(event_name = "ConsultOffer", 1, 0)) as consult,
        sum(if(event_name = "BookingConfirmation", 1, 0)) as booking
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_similar_offer_events`
    WHERE
        event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
        AND event_name in ("ConsultOffer", "BookingConfirmation")
    GROUP BY
        1,
        2,
        3,
        4
),
transactions AS (
    SELECT
        e.event_date,
        e.user_id,
        e.offer_id,
        e.origin_offer_id,
        i.booking_id,
        any_value(venue_iris_centroid) as venue_iris_centroid,
        avg(item_rank) as item_rank,
        sum(i.booking) as booking,
        sum(i.consult) as consult,
        sum(s.seen) as seen
    FROM
        events e
        LEFT JOIN seen s ON s.event_date = e.event_date
        AND s.user_id = e.user_id
        AND s.origin_offer_id = e.origin_offer_id
        LEFT JOIN interact i ON i.event_date = e.event_date
        AND i.user_id = e.user_id
        AND i.offer_id = e.offer_id
    WHERE
        s.seen > 0
        OR i.booking > 0
        OR i.consult > 0
    GROUP BY
        1,
        2,
        3,
        4,
        5
),
features AS (
    SELECT
        ul.* EXCEPT (booking, consult, seen),
        coalesce(ul.booking > 0) as booking,
        coalesce(
            ul.consult > 0
            or ul.booking > 0
        ) as consult,
        coalesce(ul.seen > 0) as seen,
        d.delta_diversification,
        od.* EXCEPT (offer_id),
        ST_DISTANCE(
            ul.venue_iris_centroid,
            ST_GEOGPOINT(od.venue_longitude, od.venue_latitude)
        ) as user_distance,
        date_diff(offer_creation_date, event_date, DAY) as offer_creation_days,
        date_diff(stock_beginning_date, event_date, DAY) as stock_beginning_days
    FROM
        transactions ul
        JOIN offer_details od on od.offer_id = ul.offer_id
        LEFT JOIN diversification d on d.booking_id = ul.booking_id
)
SELECT
    *
FROM
    features