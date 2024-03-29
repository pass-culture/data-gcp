WITH all_bookings_reconciled AS (
  SELECT
    booking.user_id
    , COALESCE(event_date, DATE(booking_creation_date)) AS booking_date
    , COALESCE(event_timestamp, TIMESTAMP(booking_creation_date)) AS booking_timestamp
    , session_id AS booking_session_id
    , unique_session_id AS booking_unique_session_id
    , booking.offer_id
    , booking.deposit_id
    , booking.booking_status
    , booking.booking_is_cancelled
    , booking.booking_intermediary_amount
    , item_id
    , booking.booking_id
    , platform
    , search_id
    , f_events.user_location_type
  FROM
      `{{ bigquery_clean_dataset }}.applicative_database_booking` booking
  LEFT JOIN `{{ bigquery_analytics_dataset }}.firebase_events` f_events
  ON f_events.booking_id = booking.booking_id
  AND event_name = 'BookingConfirmation'
  AND event_date = DATE('{{ add_days(ds, params.to) }}')
  INNER JOIN `{{ bigquery_clean_dataset }}.offer_item_ids` offer_item_ids
  ON offer_item_ids.offer_id = booking.offer_id
  WHERE DATE(booking_creation_date) = DATE('{{ add_days(ds, params.to) }}')
)

, firebase_consult AS (
  SELECT
    user_id
    , offer_id
    , item_id
    , event_date AS consult_date
    , event_timestamp AS consult_timestamp
    , origin AS consult_origin
    , reco_call_id
    , module_id
    , module_name
    , entry_id
  FROM `{{ bigquery_analytics_dataset }}.firebase_events`
  INNER JOIN `{{ bigquery_clean_dataset }}.offer_item_ids` offer_item_ids USING(offer_id)
  WHERE event_name = 'ConsultOffer'
  AND origin NOT IN ('offer', 'endedbookings','bookingimpossible', 'bookings')
  AND event_date BETWEEN DATE('{{ add_days(ds, params.from) }}') AND DATE('{{ add_days(ds, params.to) }}')  
)

, bookings_origin_first_touch AS (
  SELECT 
    all_bookings_reconciled.user_id
    , booking_date
    , booking_timestamp
    , booking_session_id
    , booking_unique_session_id
    , reco_call_id
    , all_bookings_reconciled.offer_id
    , all_bookings_reconciled.item_id
    , booking_id
    , consult_date
    , consult_timestamp
    , consult_origin AS consult_origin_first_touch
    , platform
    , search_id
    , all_bookings_reconciled.user_location_type
    , module_id AS module_id_first_touch
    , module_name AS module_name_first_touch
    , entry_id
  FROM all_bookings_reconciled
  LEFT JOIN firebase_consult
  ON all_bookings_reconciled.user_id = firebase_consult.user_id
  AND all_bookings_reconciled.item_id = firebase_consult.item_id
  AND consult_date >= DATE_SUB(booking_date, INTERVAL 7 DAY)
  AND consult_timestamp < booking_timestamp
  QUALIFY ROW_NUMBER() OVER(PARTITION BY firebase_consult.user_id, firebase_consult.item_id ORDER BY consult_timestamp ) = 1
)

, bookings_origin_last_touch AS (
  SELECT 
      all_bookings_reconciled.user_id
    , booking_date
    , booking_timestamp
    , booking_session_id
    , booking_unique_session_id
    , reco_call_id
    , all_bookings_reconciled.offer_id
    , all_bookings_reconciled.item_id
    , booking_id
    , consult_date
    , consult_timestamp
    , consult_origin AS consult_origin_last_touch
    , platform
    , search_id
    , module_id AS module_id_last_touch
    , module_name AS module_name_last_touch
  FROM all_bookings_reconciled
  LEFT JOIN firebase_consult
  ON all_bookings_reconciled.user_id = firebase_consult.user_id
  AND all_bookings_reconciled.item_id = firebase_consult.item_id
  AND consult_date >= DATE_SUB(booking_date, INTERVAL 7 DAY)
  AND consult_timestamp < booking_timestamp
  QUALIFY ROW_NUMBER() OVER(PARTITION BY firebase_consult.user_id, firebase_consult.item_id ORDER BY consult_timestamp DESC) = 1
)

, booking_origin AS (
  SELECT 
  all_bookings_reconciled.user_id
  , all_bookings_reconciled.booking_date
  , all_bookings_reconciled.booking_timestamp
  , all_bookings_reconciled.booking_session_id
  , all_bookings_reconciled.booking_unique_session_id
  , first_t.reco_call_id
  , all_bookings_reconciled.offer_id
  , all_bookings_reconciled.item_id
  , all_bookings_reconciled.booking_id
  , all_bookings_reconciled.deposit_id
  , all_bookings_reconciled.booking_status
  , all_bookings_reconciled.booking_is_cancelled
  , all_bookings_reconciled.booking_intermediary_amount
  , first_t.consult_date
  , first_t.consult_timestamp
  , consult_origin_first_touch
  , consult_origin_last_touch
  , first_t.platform
  , first_t.search_id
  , module_id_first_touch
  , module_name_first_touch
  , module_id_last_touch
  , module_name_last_touch
  , entry_id
FROM all_bookings_reconciled
LEFT JOIN bookings_origin_first_touch AS first_t USING(booking_id)
LEFT JOIN bookings_origin_last_touch AS last_t USING(booking_id)
)

, mapping_module AS (
  SELECT * 
  FROM `{{ bigquery_analytics_dataset }}.contentful_homepages` 
  QUALIFY RANK() OVER(PARTITION BY module_id, home_id ORDER BY date DESC) = 1
)

SELECT 
    user_id
  , deposit_id
  , booking_date
  , booking_timestamp
  , booking_session_id
  , booking_unique_session_id
  , reco_call_id
  , offer_id
  , item_id
  , booking_id
  , booking_status
  , booking_is_cancelled
  , booking_intermediary_amount
  , consult_date
  , consult_timestamp
  , consult_origin_first_touch
  , consult_origin_last_touch
  , platform
  , search_id
  , module_id_first_touch
  , coalesce(booking_origin.module_name_first_touch, map.module_name) AS module_name_first_touch
  , module_id_last_touch
  , coalesce(booking_origin.module_name_last_touch, map2.module_name) AS module_name_last_touch
  , coalesce(entry_id, map.home_id) AS home_id
  , map.content_type

FROM booking_origin
LEFT JOIN mapping_module AS map
ON booking_origin.module_id_first_touch = map.module_id
AND booking_origin.entry_id = map.home_id
LEFT JOIN mapping_module AS map2
ON booking_origin.module_id_last_touch = map2.module_id
AND booking_origin.entry_id = map2.home_id
