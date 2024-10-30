WITH bookings AS (
    SELECT
        booking.user_id,
        booking.offer_id,
        booking.booking_creation_date AS event_date,
        booking.booking_created_at AS event_timestamp,
        EXTRACT(HOUR FROM booking.booking_created_at) AS event_hour,
        EXTRACT(DAYOFWEEK FROM booking.booking_created_at) AS event_day,
        EXTRACT(MONTH FROM booking.booking_created_at) AS event_month
    FROM `{{ bigquery_analytics_dataset }}`.`global_booking` booking
    WHERE booking.booking_creation_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 6 MONTH)
),
previous_bookings AS (
    SELECT 
        user_id,
        STRING_AGG(enroffer.item_id ORDER BY event_timestamp DESC LIMIT 10) AS previous_item_id,
        event_date,
        event_timestamp,
        event_hour,
        event_day,
        event_month
    FROM bookings
    JOIN `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    ON enroffer.offer_id = bookings.offer_id
    GROUP BY user_id, event_date,event_timestamp, event_hour, event_day, event_month
)
SELECT
    bookings.user_id,
    COALESCE(CAST(enruser.user_age AS INT64), 0) AS user_age,
    "BOOKING" AS event_type,
    bookings.event_date,
    bookings.event_hour,
    bookings.event_day,
    bookings.event_month,
    cast(unix_seconds(timestamp(bookings.event_timestamp)) as int64) as timestamp,
    enroffer.item_id AS item_id,
    enroffer.offer_subcategory_id AS offer_subcategory_id,
    enroffer.offer_category_id AS offer_category_id,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
    previous_bookings.previous_item_id
FROM bookings
JOIN `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    ON enroffer.offer_id = bookings.offer_id
INNER JOIN `{{ bigquery_analytics_dataset }}`.`global_user` enruser
    ON enruser.user_id = bookings.user_id
LEFT JOIN previous_bookings
    ON bookings.user_id = previous_bookings.user_id 
    AND bookings.event_date = previous_bookings.event_date
    AND bookings.event_timestamp = previous_bookings.event_timestamp
    AND bookings.event_hour = previous_bookings.event_hour
    AND bookings.event_day = previous_bookings.event_day
    AND bookings.event_month = previous_bookings.event_month