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
FROM {{ ref('firebase_events_analytics') }}
WHERE event_name = "BookingConfirmation"
AND event_date = 
    {% if env_var('FIREBASE_DAG_TYPE') == 'intraday' %} '{{ ds() }}'
    {% else %} DATE_SUB("{{ ds() }}", INTERVAL 1 DAY)
    {% endif %}
