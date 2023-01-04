WITH firebase_bookings AS (
  SELECT
    user_id
    , event_date AS booking_date
    , event_timestamp AS booking_timestamp
    , session_id AS booking_session_id
    , offer_id
    , booking_id
    , platform
    , search_id
  FROM
      `{{ bigquery_analytics_dataset }}.firebase_events` f_events
  WHERE
      event_name = 'BookingConfirmation'
  -- Calculer sur les 4 derniers mois
  AND event_date >= DATE('{{ add_days(ds, -120) }}')
)

, firebase_consult AS (
  SELECT
    user_id
    , offer_id
    , event_date AS consult_date
    , event_timestamp AS consult_timestamp
    , origin AS consult_origin
    , reco_call_id
    , module_id
    , module_name
    , entry_id
  FROM `{{ bigquery_analytics_dataset }}.firebase_events`
  WHERE event_name = 'ConsultOffer'
  AND event_date >= DATE('{{ add_days(ds, -127) }}')
)

, bookings_origin_first_touch AS (
  SELECT 
    firebase_bookings.user_id
    , booking_date
    , booking_timestamp
    , booking_session_id
    , reco_call_id
    , firebase_bookings.offer_id
    , booking_id
    , consult_date
    , consult_timestamp
    , consult_origin AS consult_origin_first_touch
    , platform
    , search_id
    , module_id
    , module_name
    , entry_id
  FROM firebase_bookings
  LEFT JOIN firebase_consult
  ON firebase_bookings.user_id = firebase_consult.user_id
  AND firebase_bookings.offer_id = firebase_consult.offer_id
  AND consult_date >= DATE_SUB(booking_date, INTERVAL 7 DAY)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY firebase_consult.user_id, firebase_consult.offer_id ORDER BY consult_timestamp ) = 1 
)

, bookings_origin_last_touch AS (
  SELECT 
      firebase_bookings.user_id
    , booking_date
    , booking_timestamp
    , booking_session_id
    , reco_call_id
    , firebase_bookings.offer_id
    , booking_id
    , consult_date
    , consult_timestamp
    , consult_origin AS consult_origin_last_touch
    , platform
    , search_id
  FROM firebase_bookings
  LEFT JOIN firebase_consult
  ON firebase_bookings.user_id = firebase_consult.user_id
  AND firebase_bookings.offer_id = firebase_consult.offer_id
  AND consult_date >= DATE_SUB(booking_date, INTERVAL 7 DAY)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY firebase_consult.user_id, firebase_consult.offer_id ORDER BY consult_timestamp DESC) = 1 
)

, booking_origin AS (
  SELECT 
  first_t.user_id
  , first_t.booking_date
  , first_t.booking_timestamp
  , first_t.booking_session_id
  , first_t.reco_call_id
  , first_t.offer_id
  , first_t.booking_id
  , first_t.consult_date
  , first_t.consult_timestamp
  , consult_origin_first_touch
  , consult_origin_last_touch
  , first_t.platform
  , first_t.search_id
  , module_id
  , module_name
  , entry_id
FROM bookings_origin_first_touch AS first_t
JOIN bookings_origin_last_touch AS last_t
ON first_t.user_id = last_t.user_id
AND first_t.offer_id = last_t.offer_id
)

, mapping_module AS (
  SELECT * 
  FROM `{{ bigquery_analytics_dataset }}.contentful_homepages` 
  QUALIFY RANK() OVER(PARTITION BY module_id, home_id ORDER BY date DESC) = 1
)

SELECT 
    user_id
  , booking_date
  , booking_timestamp
  , booking_session_id
  , reco_call_id
  , offer_id
  , booking_id
  , consult_date
  , consult_timestamp
  , consult_origin_first_touch
  , consult_origin_last_touch
  , platform
  , search_id
  , booking_origin.module_id
  , coalesce(booking_origin.module_name, map.module_name) AS module_name
  , coalesce(entry_id, home_id) AS home_id
  , content_type
FROM booking_origin
LEFT JOIN mapping_module AS map
ON booking_origin.module_id = map.module_id
AND booking_origin.entry_id = map.home_id

