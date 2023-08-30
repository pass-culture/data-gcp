SELECT
    module_displayed_date
    , destination_entry_id AS home_id
    , destination_entry_name AS home_name
    , COALESCE(playlist_id, parent_module_id) AS module_id
    , COALESCE(playlist_name, parent_module_name) AS module_name
    , COALESCE(content_type, parent_module_type)  AS module_type
    , COALESCE(user_role, 'Grand Public') AS user_role
    , COUNT(DISTINCT CONCAT(session_id, user_pseudo_id)) AS nb_sesh_display
    , COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL OR click_type IS NOT NULL OR consult_venue_timestamp IS NOT NULL THEN CONCAT(session_id, user_pseudo_id) ELSE NULL END) AS nb_sesh_click
    , COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL THEN CONCAT(session_id, user_pseudo_id) ELSE NULL END) AS nb_sesh_consult_offer
    , COUNT(DISTINCT CASE WHEN click_type = 'ConsultVideo' THEN CONCAT(session_id, user_pseudo_id) ELSE NULL END) AS nb_sesh_consult_video
    , COUNT( CASE WHEN consult_offer_timestamp IS NOT NULL THEN 1 ELSE NULL END) AS nb_consult_offer
    , COUNT(DISTINCT CASE WHEN booking_timestamp IS NOT NULL THEN CONCAT(session_id, user_pseudo_id) ELSE NULL END) AS nb_sesh_booking
    , COUNT( CASE WHEN booking_timestamp IS NOT NULL THEN 1 ELSE NULL END) AS nb_bookings
    , COUNT( CASE WHEN booking_id IS NOT NULL THEN 1 ELSE NULL END) AS nb_bookings_non_cancelled
    , SUM(delta_diversification) AS total_diversification
FROM `{{ bigquery_analytics_dataset }}.firebase_home_funnel_conversion`
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_user` USING(user_id)
LEFT JOIN `{{ bigquery_analytics_dataset }}.diversification_booking`  USING(booking_id)
GROUP BY
    module_displayed_date
    , destination_entry_id
    , destination_entry_name
    , COALESCE(playlist_id, parent_module_id)
    , COALESCE(playlist_name, parent_module_name)
    , COALESCE(content_type, parent_module_type)
    , COALESCE(user_role, 'Grand Public')