SELECT 
    user_id
    , user_pseudo_id
    , session_id
    , offer_id
    , booking_id
    , event_date as booking_date
    , event_timestamp as booking_timestamp
FROM `{{ bigquery_analytics_dataset }}.firebase_events`
WHERE event_name = "BookingConfirmation"
  {% if params.dag_type == 'intraday' %}
  AND event_date = DATE('{{ ds }}')        
  {% else %}
  AND event_date = DATE('{{ add_days(ds, -1) }}')
  {% endif %}