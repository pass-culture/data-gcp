WITH users_expired_monthly AS (
SELECT 
    deposit.deposit_expiration_date,
    user.user_id,
    user.user_is_priority_public,
    user.user_is_in_qpv,
    user.user_is_unemployed,
    user.user_density_label,
    user.user_macro_density_label,
    -- user.user_region_name,
    -- user.user_department_code,
    -- user.user_activity,
    -- user.user_civility,
    user.is_theme_subscribed,
    -- user.currently_subscribed_themes,
    deposit.deposit_source,
    deposit.deposit_rank_desc,
    deposit.total_actual_amount_spent,
    deposit.total_theoretical_amount_spent_in_digital_goods,
    deposit.total_non_cancelled_individual_bookings,
    deposit.first_individual_booking_date,
    deposit.deposit_creation_date 
FROM {{ ref("mrt_global__user") }} user 
JOIN {{ ref("mrt_global__deposit") }} deposit ON user.user_id = deposit.user_id and deposit.deposit_type = "GRANT_18" AND deposit.deposit_expiration_date < DATE_TRUNC(current_date, MONTH)
)

, bookings_info AS (
SELECT 
    user.deposit_expiration_date,
    user.user_id,
    COUNT(DISTINCT CASE WHEN book.booking_quantity = 2 THEN book.booking_id END) duo_booking,
    COUNT(DISTINCT book.offer_category_id) total_category_booked,
    SUM(delta_diversification) total_diversification,
    SUM(venue_id_diversification) total_venue_id_diversification,
    SUM(venue_type_label_diversification) total_venue_type_label_diversification
FROM {{ ref("mrt_global__booking") }} book  
JOIN users_expired_monthly user ON user.user_id = book.user_id 
JOIN {{ ref("diversification_booking") }} ON book.booking_id = diversification_booking.booking_id 
WHERE NOT booking_is_cancelled 
GROUP BY 1, 2 
)

, free_booking AS (
SELECT 
    deposit_expiration_date,
    users_expired_monthly.user_id,
    COUNT(DISTINCT booking_id) AS total_free_bookings
FROM {{ ref("mrt_global__booking") }} 
JOIN {{ ref("mrt_global__offer") }} on global_offer.offer_id = global_booking.offer_id AND global_offer.last_stock_price = 0 
JOIN users_expired_monthly on global_booking.user_id = users_expired_monthly.user_id 
GROUP BY 1, 2 
)

, wau_compute as (
SELECT 
    deposit_expiration_date,
    DATE_TRUNC(visits.first_event_date, week) connexion_week, 
    count(distinct visits.user_id) weekly_connected_users 
FROM {{ ref("firebase_visits") }} visits 
JOIN users_expired_monthly ON visits.user_id = users_expired_monthly.user_id AND DATE_SUB(deposit_expiration_date, INTERVAL 2 YEAR) <= visits.first_event_date AND deposit_expiration_date >= visits.first_event_date 
GROUP BY 1, 2 
)

, wau as (
SELECT 
    deposit_expiration_date,
    avg(weekly_connected_users) AS WAU 
FROM wau_compute 
GROUP BY 1 
)

, mau_compute as (
SELECT 
    deposit_expiration_date,
    DATE_TRUNC(first_event_date, month) connexion_month, 
    count(distinct visits.user_id) monthly_connected_users 
FROM {{ ref("firebase_visits") }} visits 
JOIN users_expired_monthly ON visits.user_id = users_expired_monthly.user_id AND DATE_SUB(deposit_expiration_date, INTERVAL 2 YEAR) <= visits.first_event_date AND deposit_expiration_date >= visits.first_event_date 
GROUP BY 1, 2 
)

, mau as (
SELECT 
    deposit_expiration_date,
    avg(monthly_connected_users) AS MAU 
FROM mau_compute 
GROUP BY 1 
)

, consultations as (
SELECT 
    u.deposit_expiration_date,
    u.user_id,
    SUM(c.item_discovery_score) AS total_item_consulted,
    SUM(c.category_discovery_score) AS total_category_consulted,
    COUNT(DISTINCT c.venue_id) AS total_venue_consulted,
    COUNT(DISTINCT c.venue_type_label) AS total_venue_type_label_consulted 
FROM analytics_prod.native_consultation c  
JOIN users_expired_monthly u ON c.user_id = u.user_id AND DATE_SUB(deposit_expiration_date, INTERVAL 2 YEAR) <= c.consultation_date AND deposit_expiration_date >= c.consultation_date 
GROUP BY 1, 2 
)


SELECT 
    DATE_TRUNC(u.deposit_expiration_date, MONTH) AS expiration_month,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN u.user_is_in_qpv THEN u.user_id END) AS total_qpv_users,
    COUNT(DISTINCT CASE WHEN u.user_is_unemployed THEN u.user_id END) AS total_unemployed_users,
    COUNT(DISTINCT CASE WHEN u.user_macro_density_label = "rural" THEN u.user_id END) AS total_rural_users,
    COUNT(DISTINCT CASE WHEN u.user_is_priority_public THEN u.user_id END) AS total_priority_public_users,
    COUNT(DISTINCT CASE WHEN u.is_theme_subscribed THEN u.user_id END) AS total_theme_subscribed_users,
    COUNT(DISTINCT CASE WHEN u.deposit_rank_desc > 1 THEN u.user_id END) AS total_pre_grant_18_users,
    AVG(u.total_actual_amount_spent) AS avg_amount_spent,
    AVG(u.total_theoretical_amount_spent_in_digital_goods) AS avg_theoretical_amount_spent_in_digital_goods,
    AVG(u.total_non_cancelled_individual_bookings) AS avg_non_cancelled_individual_bookings,
    AVG(b.total_category_booked) AS avg_category_booked,
    COUNT(DISTINCT CASE WHEN b.total_category_booked >= 3 THEN u.user_id END) total_3_category_booked_users,
    AVG(DATE_DIFF(u.first_individual_booking_date, u.deposit_creation_date, DAY)) AS avg_day_between_deposit_and_first_booking,
    AVG(f.total_free_bookings) AS avg_free_bookings,
    AVG(b.total_diversification) AS avg_diversification_score,
    AVG(b.total_venue_id_diversification) AS avg_venue_id_diversification_score,
    AVG(b.total_venue_type_label_diversification) AS avg_venue_type_label_diversification_score,
    AVG(wau.wau) AS wau,
    AVG(mau.mau) AS mau,
    AVG(total_item_consulted) AS avg_item_consulted,
    AVG(total_venue_consulted) AS avg_venue_consulted,
    AVG(total_venue_type_label_consulted) AS avg_venue_type_label_consulted
FROM users_expired_monthly u 
LEFT JOIN bookings_info b on u.user_id = b.user_id AND u.deposit_expiration_date = b.deposit_expiration_date 
LEFT JOIN free_booking f on f.user_id = u.user_id AND f.deposit_expiration_date = u.deposit_expiration_date
LEFT JOIN wau on wau.deposit_expiration_date = u.deposit_expiration_date
LEFT JOIN mau on mau.deposit_expiration_date = u.deposit_expiration_date 
LEFT JOIN consultations c on u.user_id = c.user_id AND u.deposit_expiration_date = c.deposit_expiration_date 
GROUP BY 1 