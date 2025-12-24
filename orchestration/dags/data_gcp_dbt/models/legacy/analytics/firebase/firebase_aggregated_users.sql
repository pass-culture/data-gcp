select
    user_id,
    min(first_event_timestamp) as first_connexion_date,
    max(last_event_timestamp) as last_connexion_date,
    sum(visit_duration_seconds) as visit_total_time,
    avg(visit_duration_seconds) as visit_avg_time,
    count(distinct session_id) as visit_count,
    sum(nb_screen_view) as screen_view,
    sum(nb_screen_view_home) as screen_view_home,
    sum(nb_screen_view_search) as screen_view_search,
    sum(nb_screen_view_offer) as screen_view_offer,
    sum(nb_screen_view_profile) as screen_view_profile,
    sum(nb_screen_view_favorites) as screen_view_favorites,
    sum(nb_share) as share,
    sum(nb_consult_video) as consult_video,
    sum(nb_add_to_favorites) as has_added_offer_to_favorites,
    sum(nb_consult_offer) as consult_offer,
    sum(nb_booking_confirmation) as booking_confirmation
from {{ ref("firebase_visits") }}

where user_id is not null
group by user_id
