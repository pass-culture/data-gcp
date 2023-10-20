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
    , COALESCE(AVG(diversification_booking.delta_diversification),0) AS avg_diversification
    , COUNT(DISTINCT CASE WHEN nb_signup_completed > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_signup
    , COUNT(DISTINCT CASE WHEN nb_benef_request_sent > 0 THEN firebase_visits.unique_session_id ELSE NULL END) AS nb_benef_request_sent
 FROM `{{ bigquery_analytics_dataset }}`.firebase_visits
 INNER JOIN `{{ bigquery_analytics_dataset }}`.firebase_session_origin ON firebase_session_origin.unique_session_id = firebase_visits.unique_session_id
                                                    AND firebase_session_origin.traffic_campaign IS NOT NULL
LEFT JOIN `{{ bigquery_analytics_dataset }}`.aggregated_daily_user_used_activity daily_activity ON daily_activity.user_id = firebase_visits.user_id
                                                                            AND daily_activity.active_date = DATE(firebase_visits.first_event_timestamp)
LEFT JOIN `{{ bigquery_analytics_dataset }}`.firebase_bookings ON firebase_bookings.unique_session_id = firebase_visits.unique_session_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.diversification_booking ON diversification_booking.booking_id = firebase_bookings.booking_id
GROUP BY 1,2,3,4,5,6,7