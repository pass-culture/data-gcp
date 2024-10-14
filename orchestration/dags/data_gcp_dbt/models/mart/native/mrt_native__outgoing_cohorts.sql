with
    users_expired_monthly as (
        select
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
            date_diff(
                last_deposit_expiration_date, first_deposit_creation_date, day
            ) as seniority_days
        from {{ ref("mrt_global__user") }} user
        where
            current_deposit_type = "GRANT_18"
            and last_deposit_expiration_date < date_trunc(current_date, month)
    ),
    bookings_info as (
        select
            user.deposit_expiration_date,
            user.user_id,
            sum(delta_diversification) as total_diversification,
            sum(venue_id_diversification) as total_venue_id_diversification,
            sum(
                venue_type_label_diversification
            ) as total_venue_type_label_diversification,
            sum(category_diversification) as total_category_diversification
        from users_expired_monthly user
        join
            {{ ref("diversification_booking") }} diversification_booking
            on user.user_id = diversification_booking.user_id
        group by deposit_expiration_date, user_id
    ),
    weekly_active_user_compute as (
        select
            deposit_expiration_date,
            date_trunc(visits.first_event_date, week) connexion_week,
            count(distinct visits.user_id) weekly_connected_users
        from {{ ref("firebase_visits") }} visits
        join
            users_expired_monthly
            on visits.user_id = users_expired_monthly.user_id
            and date_sub(deposit_expiration_date, interval 2 year)
            <= visits.first_event_date
            and deposit_expiration_date >= visits.first_event_date
        group by deposit_expiration_date, connexion_week
    ),
    weekly_active_user as (
        select
            deposit_expiration_date, avg(weekly_connected_users) as weekly_active_user
        from weekly_active_user_compute
        group by deposit_expiration_date
    ),
    monthly_active_user_compute as (
        select
            deposit_expiration_date,
            date_trunc(first_event_date, month) connexion_month,
            count(distinct visits.user_id) monthly_connected_users
        from {{ ref("firebase_visits") }} visits
        join
            users_expired_monthly
            on visits.user_id = users_expired_monthly.user_id
            and date_sub(deposit_expiration_date, interval 2 year)
            <= visits.first_event_date
            and deposit_expiration_date >= visits.first_event_date
        group by deposit_expiration_date, connexion_month
    ),
    monthly_active_user as (
        select
            deposit_expiration_date, avg(monthly_connected_users) as monthly_active_user
        from monthly_active_user_compute
        group by deposit_expiration_date
    ),
    consultations as (
        select
            u.deposit_expiration_date,
            u.user_id,
            sum(c.item_discovery_score) as total_item_consulted,
            sum(c.category_discovery_score) as total_category_consulted,
            count(distinct c.venue_id) as total_venue_consulted,
            count(distinct c.venue_type_label) as total_venue_type_label_consulted
        from {{ ref("mrt_native__consultation") }} c
        join
            users_expired_monthly u
            on c.user_id = u.user_id
            and date_sub(deposit_expiration_date, interval 2 year)
            <= c.consultation_date
            and deposit_expiration_date >= c.consultation_date
        group by deposit_expiration_date, user_id
    )

select
    date_trunc(u.deposit_expiration_date, month) as expiration_month,
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
    count(distinct u.user_id) as total_users,
    count(
        distinct case when b.total_category_diversification >= 3 then u.user_id end
    ) total_3_category_booked_users,
    sum(u.total_actual_amount_spent) as total_amount_spent,
    sum(
        u.total_theoretical_digital_goods_amount_spent
    ) as total_theoretical_digital_goods_amount_spent,
    sum(
        u.total_non_cancelled_individual_bookings
    ) as total_non_cancelled_individual_bookings,
    sum(u.total_non_cancelled_duo_bookings) as total_non_cancelled_duo_bookings,
    sum(u.total_free_bookings) as total_free_bookings,
    sum(total_item_consulted) as total_item_consulted,
    sum(total_venue_consulted) as total_venue_consulted,
    avg(u.total_distinct_grant_18_booking_types) as avg_grant_18_subcategory_booked,
    avg(
        u.total_distinct_grant_15_17_booking_types
    ) as avg_grant_15_17_subcategory_booked,
    avg(total_venue_type_label_consulted) as avg_venue_type_label_consulted,
    avg(
        date_diff(u.first_individual_booking_date, u.first_deposit_creation_date, day)
    ) as avg_day_between_deposit_and_first_booking,
    avg(b.total_diversification) as avg_diversification_score,
    avg(b.total_venue_id_diversification) as avg_venue_id_diversification_score,
    avg(
        b.total_venue_type_label_diversification
    ) as avg_venue_type_label_diversification_score,
    avg(b.total_category_diversification) as avg_category_diversification_score,
    avg(weekly_active_user.weekly_active_user) as weekly_active_user,
    avg(monthly_active_user.monthly_active_user) as monthly_active_user
from users_expired_monthly u
left join
    weekly_active_user
    on weekly_active_user.deposit_expiration_date = u.deposit_expiration_date
left join
    monthly_active_user
    on monthly_active_user.deposit_expiration_date = u.deposit_expiration_date
left join
    consultations c
    on u.user_id = c.user_id
    and u.deposit_expiration_date = c.deposit_expiration_date
left join
    bookings_info b
    on u.user_id = b.user_id
    and u.deposit_expiration_date = b.deposit_expiration_date
group by
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
