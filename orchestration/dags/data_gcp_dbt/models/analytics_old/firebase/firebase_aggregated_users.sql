SELECT
    user_id,
    MIN(first_event_timestamp) AS first_connexion_date,
    MAX(last_event_timestamp) AS last_connexion_date,
    SUM(visit_duration_seconds) AS visit_total_time,
    AVG(visit_duration_seconds) AS visit_avg_time,
    COUNT(DISTINCT session_id) AS visit_count,
    SUM(nb_screen_view) AS screen_view,
    SUM(nb_screen_view_home) AS screen_view_home,
    SUM(nb_screen_view_search) AS screen_view_search,
    SUM(nb_screen_view_offer) AS screen_view_offer,
    SUM(nb_screen_view_profile) AS screen_view_profile,
    SUM(nb_screen_view_favorites) AS screen_view_favorites,
    SUM(nb_share) AS share,
    SUM(nb_consult_video) AS consult_video,
    SUM(nb_add_to_favorites) AS has_added_offer_to_favorites,
    SUM(nb_consult_offer) AS consult_offer,
    SUM(nb_booking_confirmation) AS booking_confirmation,
FROM
        {{ ref('firebase_visits') }}
WHERE
    user_id IS NOT NULL
GROUP BY
    user_id