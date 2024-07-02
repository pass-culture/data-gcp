{{ config(
    materialized='table',
    partition_by={
        'field': 'module_displayed_date',
        'data_type': 'date',
        'granularity': 'day'
    }
) }}

SELECT
    uh.module_displayed_date,
    uh.module_id,
    uh.module_name,
    uh.module_type,
    uh.entry_id,
    uh.entry_name,
    uh.parent_module_id,
    uh.parent_module_type,
    uh.parent_entry_id,
    uh.app_version,
    uh.parent_home_type,
    uh.home_audience,
    uh.user_lifecycle_home,
    uh.home_type,
    uh.playlist_type,
    uh.offer_category,
    uh.playlist_reach,
    uh.playlist_recurrence,
    COALESCE(user_role, 'Grand Public') AS user_role,
    COUNT(DISTINCT unique_session_id) AS total_session_display,
    COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL OR click_type IS NOT NULL OR consult_venue_timestamp IS NOT NULL THEN uh.unique_session_id END) AS total_session_with_click,
    COUNT(DISTINCT CASE WHEN consult_offer_timestamp IS NOT NULL THEN uh.unique_session_id END) AS total_sesh_consult_offer,
    COUNT(DISTINCT CASE WHEN fav_timestamp IS NOT NULL THEN uh.unique_session_id END) AS total_session_fav,
    COUNT(DISTINCT CASE WHEN click_type = 'ConsultVideo' THEN uh.unique_session_id END) AS total_session_with_consult_video,
    COUNT(CASE WHEN consult_offer_timestamp IS NOT NULL THEN 1 END) AS total_consult_offer,
    COUNT(CASE WHEN fav_timestamp IS NOT NULL THEN 1 END ) AS total_fav,
    COUNT(DISTINCT CASE WHEN booking_timestamp IS NOT NULL THEN uh.unique_session_id END) AS total_session_with_booking,
    COUNT(CASE WHEN booking_timestamp IS NOT NULL THEN 1 END) AS total_bookings,
    COUNT(CASE WHEN uh.booking_id IS NOT NULL THEN 1 END ) AS total_non_cancelled_bookings,
    SUM(db.delta_diversification) AS total_diversification
FROM {{ ref( 'mrt_native__daily_user_home_module' ) }} AS uh
LEFT JOIN {{ ref('int_applicative__user') }} AS u
    ON u.user_id = uh.user_id
LEFT JOIN {{ ref('diversification_booking') }} AS db
    ON db.booking_id = uh.booking_id
WHERE module_displayed_date >= date_sub(DATE('{{ ds() }}'), INTERVAL 6 MONTH)

GROUP BY
    module_displayed_date,
    module_id,
    module_name,
    module_type,
    entry_id,
    entry_name,
    parent_module_id,
    parent_module_type,
    parent_entry_id,
    app_version,
    parent_home_type,
    home_audience,
    user_lifecycle_home,
    home_type,
    playlist_type,
    offer_category,
    playlist_reach,
    playlist_recurrence,
    user_role
