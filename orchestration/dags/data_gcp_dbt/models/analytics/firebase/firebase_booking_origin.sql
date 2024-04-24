{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "booking_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
        cluster_by = "module_name_first_touch"
    )
}}

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
    , booking.item_id
    , booking.booking_id
    , platform
    , f_events.user_location_type
  FROM
      {{ ref('mrt_global__booking') }} booking
  LEFT JOIN {{ ref('int_firebase__native_event') }} f_events
  ON f_events.booking_id = booking.booking_id
  AND event_name = 'BookingConfirmation'
  {% if is_incremental() %}
      AND date(event_date) BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
  {% endif %}

  {% if is_incremental() %}
    WHERE DATE(booking_creation_date) BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
  {% endif %}
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
    , search_id
    , module_id
    , module_name
    , entry_id
  FROM {{ ref('int_firebase__native_event') }}
  INNER JOIN {{ ref('offer_item_ids') }} offer_item_ids USING(offer_id)
  WHERE event_name = 'ConsultOffer'
  AND origin NOT IN ('offer', 'endedbookings','bookingimpossible', 'bookings')
  {% if is_incremental() %}
    AND DATE(event_date) BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 10 DAY) and DATE('{{ ds() }}') -- 3 + 7 days lag
  {% endif %}
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
    , entry_id AS home_id_first_touch
  FROM all_bookings_reconciled
  LEFT JOIN firebase_consult
  ON all_bookings_reconciled.user_id = firebase_consult.user_id
  AND all_bookings_reconciled.item_id = firebase_consult.item_id
  AND consult_date >= DATE_SUB(booking_date, INTERVAL 7 DAY) -- force 7 days lag max
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
    , entry_id AS home_id_last_touch
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
  , first_t.search_id as search_id_first_touch
  , last_t.search_id as search_id_last_touch
  , first_t.reco_call_id as reco_call_id_first_touch
  , last_t.reco_call_id as reco_call_id_last_touch
  , module_id_first_touch
  , module_name_first_touch
  , module_id_last_touch
  , module_name_last_touch
  , home_id_last_touch
  , home_id_first_touch
FROM all_bookings_reconciled
LEFT JOIN bookings_origin_first_touch AS first_t USING(booking_id)
LEFT JOIN bookings_origin_last_touch AS last_t USING(booking_id)
)

, mapping_module AS (
  SELECT * 
  FROM {{ ref("int_contentful__homepage") }}
  QUALIFY RANK() OVER(PARTITION BY module_id, home_id ORDER BY date DESC) = 1
)

SELECT 
    user_id
  , deposit_id
  , booking_date
  , booking_timestamp
  , booking_session_id
  , booking_unique_session_id
  , offer_id
  , item_id
  , booking_id
  , booking_status
  , booking_is_cancelled
  , booking_intermediary_amount
  , consult_date
  , consult_timestamp
  -- origin
  , consult_origin_first_touch
  , consult_origin_last_touch
  , platform
  -- technical related
  , reco_call_id_first_touch
  , reco_call_id_last_touch
  , search_id_first_touch
  , search_id_last_touch
  -- home related first_touch
  , module_id_first_touch
  , coalesce(booking_origin.module_name_first_touch, first_touch_map.module_name) AS module_name_first_touch
  , coalesce(home_id_first_touch, first_touch_map.home_id) AS home_id_first_touch
  , first_touch_map.home_name AS home_name_first_touch
  , first_touch_map.content_type AS content_type_first_touch
  -- home related last_touch
  , module_id_last_touch
  , coalesce(booking_origin.module_name_last_touch, last_touch_map.module_name) AS module_name_last_touch
  , coalesce(home_id_last_touch, last_touch_map.home_id) AS home_id_last_touch
  , last_touch_map.home_name AS home_name_last_touch
  , last_touch_map.content_type AS content_type_last_touch
  
FROM booking_origin
LEFT JOIN mapping_module AS first_touch_map
ON booking_origin.module_id_first_touch = first_touch_map.module_id
AND booking_origin.home_id_first_touch = first_touch_map.home_id
LEFT JOIN mapping_module AS last_touch_map
ON booking_origin.module_id_last_touch = last_touch_map.module_id
AND booking_origin.home_id_last_touch = last_touch_map.home_id
