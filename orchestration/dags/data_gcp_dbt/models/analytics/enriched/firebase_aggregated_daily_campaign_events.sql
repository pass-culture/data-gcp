{{
  config(
    materialized = "incremental",
    partition_by={
      "field": "first_event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = 'insert_overwrite'
  )
}}

WITH bookings_and_diversification_per_sesh AS (
SELECT
    firebase_bookings.unique_session_id
    , COUNT(DISTINCT booking_id) AS booking_diversification_cnt
    , SUM(delta_diversification) AS total_delta_diversification
FROM {{ ref('firebase_bookings') }} firebase_bookings
INNER JOIN  {{ ref('diversification_booking') }} diversification_booking USING(booking_id)
GROUP BY 1
)

SELECT
    firebase_session_origin.traffic_campaign
    , firebase_session_origin.traffic_medium
    , firebase_session_origin.traffic_source
    , firebase_session_origin.traffic_gen
    , firebase_session_origin.traffic_content
    , firebase_session_origin.first_event_date AS event_date
    , COALESCE(daily_activity.deposit_type, 'Grand Public') AS user_type
    , COUNT(DISTINCT firebase_visits.unique_session_id) AS nb_sesh
    , COUNT(DISTINCT CASE WHEN nb_consult_offer > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_sesh_consult
    , COUNT(DISTINCT CASE WHEN nb_add_to_favorites > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_sesh_add_to_fav
    , COUNT(DISTINCT CASE WHEN nb_booking_confirmation > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_sesh_booking
    , COALESCE(SUM(nb_consult_offer),0) AS nb_consult_offer
    , COALESCE(SUM(nb_add_to_favorites),0) AS nb_add_to_favorites
    , COALESCE(SUM(nb_booking_confirmation),0) AS nb_booking
    , COALESCE(SAFE_DIVIDE(SUM(total_delta_diversification), SUM(booking_diversification_cnt)),0) AS avg_diversification
    , COUNT(DISTINCT CASE WHEN nb_signup_completed > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_signup
    , COUNT(DISTINCT CASE WHEN nb_benef_request_sent > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_benef_request_sent
 FROM {{ ref('firebase_visits') }} firebase_visits
 INNER JOIN {{ ref('firebase_session_origin') }} firebase_session_origin ON firebase_session_origin.unique_session_id = firebase_visits.unique_session_id
                                                    AND firebase_session_origin.traffic_campaign IS NOT NULL
LEFT JOIN {{ ref('aggregated_daily_user_used_activity') }} aggregated_daily_user_used_activity daily_activity ON daily_activity.user_id = firebase_visits.user_id
                                                                            AND daily_activity.active_date = DATE(firebase_visits.first_event_timestamp)
LEFT JOIN bookings_and_diversification_per_sesh ON bookings_and_diversification_per_sesh.unique_session_id = firebase_visits.unique_session_id
{% if is_incremental() %}
    where firebase_session_origin.first_event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 1 DAY) and DATE('{{ ds() }}')
{% endif %}
GROUP BY 1,2,3,4,5,6,7