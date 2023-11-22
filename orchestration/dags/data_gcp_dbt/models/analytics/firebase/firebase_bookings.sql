SELECT 
    user_id
    , user_pseudo_id
    , session_id
    , unique_session_id
    , offer_id
    , booking_id
    , event_date as booking_date
    , event_timestamp as booking_timestamp
    , user_location_type
FROM {{ ref('firebase_events') }}
WHERE event_name = "BookingConfirmation"
{% if env_var('FIREBASE_DAG_TYPE') == 'intraday' %}
AND event_date = '{{ ds() }}'    
{% else %}
AND event_date = DATE_SUB("{{ ds() }}", INTERVAL 1 DAY)
{% endif %}
