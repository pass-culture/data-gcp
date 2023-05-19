SELECT 
    user_id
    , session_id
    , offer_id
    , booking_id
    , event_date as booking_date
    , event_timestamp as booking_timestamp
FROM `{{ bigquery_analytics_dataset }}.firebase_events`
WHERE event_name = "BookingConfirmation"