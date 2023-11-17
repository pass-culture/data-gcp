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
  {% if params.dag_type == 'intraday' %}
  AND event_date = DATE('{{ ds }}')        
  {% else %}
  AND event_date = DATE('{{ add_days(ds, -1) }}')
  {% endif %}