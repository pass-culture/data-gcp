WITH events AS (
    SELECT
        user_id,
        offer_id,
        event_date,
        event_timestamp,
        EXTRACT(HOUR FROM event_timestamp) AS event_hour,
        EXTRACT(DAYOFWEEK FROM event_timestamp) AS event_day,
        EXTRACT(MONTH FROM event_timestamp) AS event_month
    FROM `{{ bigquery_int_firebase_dataset }}`.`native_event`
    WHERE
        event_name = "HasAddedOfferToFavorites"
        AND event_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 6 MONTH)
        AND user_id IS NOT NULL
        AND offer_id IS NOT NULL
        AND offer_id != 'NaN'
),
previous_events AS (
    SELECT 
        user_id,
        STRING_AGG(enroffer.item_id ORDER BY event_timestamp DESC LIMIT 10) AS previous_item_id,
        event_date,
        event_hour,
        event_day,
        event_month
    FROM events
    JOIN `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    ON enroffer.offer_id = events.offer_id
    GROUP BY user_id, event_date, event_hour, event_day, event_month
)
SELECT
    events.user_id,
    COALESCE(CAST(enruser.user_age AS INT64), 0) AS user_age,
    "FAVORITE" AS event_type,
    events.event_date,
    events.event_hour,
    events.event_day,
    events.event_month,
    cast(unix_seconds(timestamp(events.event_date)) as int64) as timestamp,
    enroffer.item_id AS item_id,
    enroffer.offer_subcategory_id AS offer_subcategory_id,
    enroffer.offer_category_id AS offer_category_id,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
    previous_events.previous_item_id 
FROM events
JOIN `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    ON enroffer.offer_id = events.offer_id
INNER JOIN `{{ bigquery_analytics_dataset }}`.`global_user` enruser
    ON enruser.user_id = events.user_id
LEFT JOIN previous_events
    ON events.user_id = previous_events.user_id 
    AND events.event_date = previous_events.event_date
    AND events.event_hour = previous_events.event_hour
    AND events.event_day = previous_events.event_day
    AND events.event_month = previous_events.event_month
