with
    users_expired_monthly as (
        select
            last_deposit_expiration_date as deposit_expiration_date,
            user_id,
            user_is_priority_public,
            user_is_in_qpv,
            user_is_in_education,
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
    diversification_info as (
        select
            user.deposit_expiration_date,
            user.user_id,
            sum(delta_diversification) as total_diversification
        from users_expired_monthly user
        join
            {{ ref("diversification_booking") }} diversification_booking
            on user.user_id = diversification_booking.user_id
        group by deposit_expiration_date, user_id
    ),
    bookings_info as (
        select
            user.deposit_expiration_date,
            user.user_id,
            count(
                distinct bookings.offer_category_id
            ) as total_distinct_category_booked,
            count(
                distinct bookings.venue_type_label
            ) as total_distinct_venue_type_booked,
            count(distinct bookings.venue_id) as total_distinct_venue_booked
        from users_expired_monthly user
        join
            {{ ref("mrt_global__booking") }} bookings on bookings.user_id = user.user_id
        group by deposit_expiration_date, user_id
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
    u.user_is_in_education,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_department_code,
    u.user_activity,
    u.user_civility,
    u.is_theme_subscribed,
    coalesce(count(distinct u.user_id), 0) as total_users,
    coalesce(
        count(
            distinct case when b.total_distinct_category_booked >= 3 then u.user_id end
        ),
        0
    ) total_3_category_booked_users,
    coalesce(sum(u.total_actual_amount_spent), 0) as total_amount_spent,
    coalesce(
        sum(u.total_theoretical_digital_goods_amount_spent), 0
    ) as total_theoretical_digital_goods_amount_spent,
    coalesce(
        sum(u.total_non_cancelled_individual_bookings), 0
    ) as total_non_cancelled_individual_bookings,
    coalesce(
        sum(u.total_non_cancelled_duo_bookings), 0
    ) as total_non_cancelled_duo_bookings,
    coalesce(sum(u.total_free_bookings), 0) as total_free_bookings,
    coalesce(sum(total_item_consulted), 0) as total_item_consulted,
    coalesce(sum(total_venue_consulted), 0) as total_venue_consulted,
    coalesce(
        sum(u.total_distinct_grant_18_booking_types), 0
    ) as total_grant_18_subcategory_booked,
    coalesce(
        sum(u.total_distinct_grant_15_17_booking_types), 0
    ) as total_grant_15_17_subcategory_booked,
    coalesce(
        sum(total_venue_type_label_consulted), 0
    ) as total_venue_type_label_consulted,
    coalesce(
        sum(
            date_diff(
                u.first_individual_booking_date, u.first_deposit_creation_date, day
            )
        ),
        0
    ) as total_day_between_deposit_and_first_booking,
    coalesce(sum(d.total_diversification), 0) as total_diversification_score,
    coalesce(sum(b.total_distinct_venue_booked), 0) as total_venue_id_booked,
    coalesce(sum(b.total_distinct_venue_type_booked), 0) as total_venue_type_booked,
    coalesce(sum(b.total_distinct_category_booked), 0) as total_category_booked
from users_expired_monthly u
left join
    consultations c
    on u.user_id = c.user_id
    and u.deposit_expiration_date = c.deposit_expiration_date
left join
    diversification_info d
    on u.user_id = d.user_id
    and u.deposit_expiration_date = d.deposit_expiration_date
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
    u.user_is_in_education,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_department_code,
    u.user_activity,
    u.user_civility,
    u.is_theme_subscribed
