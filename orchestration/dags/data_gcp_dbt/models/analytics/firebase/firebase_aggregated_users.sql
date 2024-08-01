select
    user_id,
    MIN(first_event_timestamp) as first_connexion_date,
    MAX(last_event_timestamp) as last_connexion_date,
    SUM(visit_duration_seconds) as visit_total_time,
    AVG(visit_duration_seconds) as visit_avg_time,
    COUNT(distinct session_id) as visit_count,
    SUM(nb_screen_view) as screen_view,
    SUM(nb_screen_view_home) as screen_view_home,
    SUM(nb_screen_view_search) as screen_view_search,
    SUM(nb_screen_view_offer) as screen_view_offer,
    SUM(nb_screen_view_profile) as screen_view_profile,
    SUM(nb_screen_view_favorites) as screen_view_favorites,
    SUM(nb_share) as share,
    SUM(nb_consult_video) as consult_video,
    SUM(nb_add_to_favorites) as has_added_offer_to_favorites,
    SUM(nb_consult_offer) as consult_offer,
    SUM(nb_booking_confirmation) as booking_confirmation
from {{ ref('firebase_visits') }}

where
    user_id is not NULL
group by
    user_id
