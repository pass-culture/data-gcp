SELECT
    module_displayed_date
    , entry_id AS home_id
    , entry_name AS home_name
    , parent_module_id
    , parent_module_name
    , parent_module_type
    , COALESCE(user_role, 'Grand Public') AS user_role
    , COUNT(DISTINCT unique_session_id) AS nb_sesh_display
    , COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL OR click_type IS NOT NULL OR consult_venue_timestamp IS NOT NULL THEN unique_session_id ELSE NULL END) AS nb_sesh_click
    , COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL THEN unique_session_id ELSE NULL END) AS nb_sesh_consult_offer
    , COUNT(DISTINCT CASE WHEN click_type = 'ConsultVideo' THEN unique_session_id ELSE NULL END) AS nb_sesh_consult_video
    , COUNT( CASE WHEN consult_offer_timestamp IS NOT NULL THEN 1 ELSE NULL END) AS nb_consult_offer
    , COUNT(DISTINCT CASE WHEN booking_timestamp IS NOT NULL THEN unique_session_id ELSE NULL END) AS nb_sesh_booking
    , COUNT( CASE WHEN booking_timestamp IS NOT NULL THEN 1 ELSE NULL END) AS nb_bookings
    , COUNT( CASE WHEN booking_id IS NOT NULL THEN 1 ELSE NULL END) AS nb_bookings_non_cancelled
    , SUM(delta_diversification) AS total_diversification
FROM {{ ref('firebase_home_funnel_conversion')}}
LEFT JOIN {{ ref('int_applicative__user') }} USING(user_id)
LEFT JOIN {{ ref('diversification_booking')}}  USING(booking_id)
GROUP BY
    module_displayed_date
    , entry_id
    ,entry_name
    ,parent_module_id
    , parent_module_name
    , parent_module_type
    , user_role