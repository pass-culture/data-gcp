SELECT
    module_displayed_date
    , entry_id AS home_id
    , entry_name AS home_name
    , COALESCE(user_role, 'Grand Public') AS user_role
    , COUNT(DISTINCT CONCAT(session_id, user_pseudo_id)) AS nb_sesh_display
    , COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL OR click_type IS NOT NULL OR consult_venue_timestamp IS NOT NULL THEN CONCAT(session_id, user_pseudo_id) ELSE NULL END) AS nb_sesh_click
    , COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL THEN CONCAT(session_id, user_pseudo_id) ELSE NULL END) AS nb_sesh_consult_offer
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
    , entry_id
    ,entry_name
    , user_role