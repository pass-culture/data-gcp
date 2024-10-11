WITH users_expired_monthly AS (
SELECT 
    last_deposit_expiration_date as deposit_expiration_date,
    user_id,
    user_is_priority_public,
    user_is_in_qpv,
    user_is_unemployed,
    user_density_label,
    user_macro_density_label,
    user_region_name,
    user_department_code,
    user_activity,
    user_civility,
    total_deposit_amount,
    is_theme_subscribed,
    total_actual_amount_spent,
    total_theoretical_digital_goods_amount_spent,
    total_non_cancelled_individual_bookings,
    total_non_cancelled_duo_bookings,
    total_free_bookings,
    total_distinct_grant_18_booking_types,
    total_distinct_grant_15_17_booking_types,
    first_individual_booking_date,
    first_deposit_creation_date,
    DATE_DIFF(last_deposit_expiration_date, first_deposit_creation_date, day) as seniority_days 
FROM {{ ref("mrt_global__user") }} user 
WHERE current_deposit_type = "GRANT_18" AND last_deposit_expiration_date < DATE_TRUNC(current_date, MONTH)
)

, bookings_info AS (
SELECT 
    user.deposit_expiration_date,
    user.user_id,
    SUM(delta_diversification) AS total_diversification,
    SUM(venue_id_diversification) AS total_venue_id_diversification,
    SUM(venue_type_label_diversification) AS total_venue_type_label_diversification,
    SUM(category_diversification) AS total_category_diversification
FROM users_expired_monthly user
JOIN {{ ref("diversification_booking") }} diversification_booking ON user.user_id = diversification_booking.user_id
GROUP BY 
    deposit_expiration_date,
    user_id 
)

, weekly_active_user_compute as (
SELECT 
    deposit_expiration_date,
    DATE_TRUNC(visits.first_event_date, week) connexion_week, 
    count(distinct visits.user_id) weekly_connected_users 
FROM {{ ref("firebase_visits") }} visits 
JOIN users_expired_monthly ON visits.user_id = users_expired_monthly.user_id AND DATE_SUB(deposit_expiration_date, INTERVAL 2 YEAR) <= visits.first_event_date AND deposit_expiration_date >= visits.first_event_date 
GROUP BY 
    deposit_expiration_date,
    connexion_week
)

, weekly_active_user as (
SELECT 
    deposit_expiration_date,
    avg(weekly_connected_users) AS weekly_active_user 
FROM weekly_active_user_compute 
GROUP BY deposit_expiration_date
)

, monthly_active_user_compute as (
SELECT 
    deposit_expiration_date,
    DATE_TRUNC(first_event_date, month) connexion_month, 
    count(distinct visits.user_id) monthly_connected_users 
FROM {{ ref("firebase_visits") }} visits 
JOIN users_expired_monthly ON visits.user_id = users_expired_monthly.user_id AND DATE_SUB(deposit_expiration_date, INTERVAL 2 YEAR) <= visits.first_event_date AND deposit_expiration_date >= visits.first_event_date 
GROUP BY 
    deposit_expiration_date,
    connexion_month
)

, monthly_active_user as (
SELECT 
    deposit_expiration_date,
    avg(monthly_connected_users) AS monthly_active_user 
FROM monthly_active_user_compute 
GROUP BY deposit_expiration_date
)

, consultations as (
SELECT 
    u.deposit_expiration_date,
    u.user_id,
    SUM(c.item_discovery_score) AS total_item_consulted,
    SUM(c.category_discovery_score) AS total_category_consulted,
    COUNT(DISTINCT c.venue_id) AS total_venue_consulted,
    COUNT(DISTINCT c.venue_type_label) AS total_venue_type_label_consulted 
FROM {{ ref("mrt_native__consultation")}} c  
JOIN users_expired_monthly u ON c.user_id = u.user_id AND DATE_SUB(deposit_expiration_date, INTERVAL 2 YEAR) <= c.consultation_date AND deposit_expiration_date >= c.consultation_date 
GROUP BY 
    deposit_expiration_date,
    user_id 
)


SELECT 
    DATE_TRUNC(u.deposit_expiration_date, MONTH) AS expiration_month,
    u.total_deposit_amount,
    u.user_is_priority_public,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_department_code,
    u.user_activity,
    u.user_civility,
    u.is_theme_subscribed,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN b.total_category_diversification >= 3 THEN u.user_id END) total_3_category_booked_users,
    SUM(u.total_actual_amount_spent) AS total_amount_spent,
    SUM(u.total_theoretical_digital_goods_amount_spent) AS total_theoretical_digital_goods_amount_spent,
    SUM(u.total_non_cancelled_individual_bookings) AS total_non_cancelled_individual_bookings,
    SUM(u.total_non_cancelled_duo_bookings) AS total_non_cancelled_duo_bookings,
    SUM(u.total_free_bookings) AS total_free_bookings,
    SUM(total_item_consulted) AS total_item_consulted,
    SUM(total_venue_consulted) AS total_venue_consulted,
    AVG(u.total_distinct_grant_18_booking_types) AS avg_grant_18_subcategory_booked,
    AVG(u.total_distinct_grant_15_17_booking_types) AS avg_grant_15_17_subcategory_booked,
    AVG(total_venue_type_label_consulted) AS avg_venue_type_label_consulted,
    AVG(DATE_DIFF(u.first_individual_booking_date, u.first_deposit_creation_date, DAY)) AS avg_day_between_deposit_and_first_booking,
    AVG(b.total_diversification) AS avg_diversification_score,
    AVG(b.total_venue_id_diversification) AS avg_venue_id_diversification_score,
    AVG(b.total_venue_type_label_diversification) AS avg_venue_type_label_diversification_score,
    AVG(b.total_category_diversification) AS avg_category_diversification_score,
    AVG(weekly_active_user.weekly_active_user) AS weekly_active_user,
    AVG(monthly_active_user.monthly_active_user) AS monthly_active_user
FROM users_expired_monthly u 
LEFT JOIN weekly_active_user on weekly_active_user.deposit_expiration_date = u.deposit_expiration_date
LEFT JOIN monthly_active_user on monthly_active_user.deposit_expiration_date = u.deposit_expiration_date 
LEFT JOIN consultations c on u.user_id = c.user_id AND u.deposit_expiration_date = c.deposit_expiration_date 
LEFT JOIN bookings_info b on u.user_id = b.user_id AND u.deposit_expiration_date = b.deposit_expiration_date
GROUP BY 
    expiration_month,
    u.total_deposit_amount,
    u.user_is_priority_public,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_department_code,
    u.user_activity,
    u.user_civility,
    u.is_theme_subscribed