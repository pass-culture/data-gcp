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
            user_department_name,
            user_activity,
            user_civility,
            total_deposit_amount,
            is_theme_subscribed as user_is_theme_subscribed,
            total_actual_amount_spent,
            total_theoretical_digital_goods_amount_spent,
            total_non_cancelled_individual_bookings,
            total_non_cancelled_duo_bookings,
            total_free_bookings,
            total_grant_18_subcategory_booked,
            total_grant_15_17_subcategory_booked,
            first_individual_booking_date,
            first_deposit_creation_date,
            date_diff(
                last_deposit_expiration_date, first_deposit_creation_date, day
            ) as seniority_days
        from {{ ref("mrt_global__user_beneficiary") }}
        where
            current_deposit_type in ("GRANT_18", "GRANT_17_18")
            and last_deposit_expiration_date < date_trunc(current_date, month)
    ),

    bookings_info as (
        select
            users_expired_monthly.deposit_expiration_date,
            users_expired_monthly.user_id,
            sum(booking.diversity_score) as total_diversification,
            count(distinct booking.offer_category_id) as total_distinct_category_booked,
            count(
                distinct booking.venue_type_label
            ) as total_distinct_venue_type_booked,
            count(
                distinct if(booking.venue_is_virtual is false, booking.venue_id, null)
            ) as total_distinct_venue_booked
        from users_expired_monthly
        inner join
            {{ ref("mrt_global__booking") }} as booking
            on users_expired_monthly.user_id = booking.user_id
        where not booking.booking_is_cancelled
        group by
            users_expired_monthly.deposit_expiration_date, users_expired_monthly.user_id
    ),

    consultations as (
        select
            u.deposit_expiration_date,
            u.user_id,
            sum(c.item_discovery_score) as total_item_consulted,
            sum(c.category_discovery_score) as total_category_consulted,
            count(distinct c.venue_id) as total_venue_consulted,
            count(distinct c.venue_type_label) as total_venue_type_label_consulted
        from {{ ref("mrt_native__consultation") }} as c
        inner join
            users_expired_monthly as u
            on c.user_id = u.user_id
            and date_sub(u.deposit_expiration_date, interval 2 year)
            <= c.consultation_date
            and u.deposit_expiration_date >= c.consultation_date
        group by u.deposit_expiration_date, u.user_id
    )

select
    u.total_deposit_amount,
    u.user_is_priority_public,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_is_in_education,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_department_code,
    u.user_department_name,
    u.user_activity,
    u.user_civility,
    u.user_is_theme_subscribed,
    date_trunc(u.deposit_expiration_date, month) as user_expiration_month,
    coalesce(count(distinct u.user_id), 0) as total_users,
    coalesce(
        count(
            distinct case when b.total_distinct_category_booked >= 3 then u.user_id end
        ),
        0
    ) as total_3_category_booked_users,
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
    coalesce(sum(c.total_item_consulted), 0) as total_item_consulted,
    coalesce(sum(c.total_venue_consulted), 0) as total_venue_consulted,
    coalesce(
        sum(u.total_grant_18_subcategory_booked), 0
    ) as total_grant_18_subcategory_booked,
    coalesce(
        sum(u.total_grant_15_17_subcategory_booked), 0
    ) as total_grant_15_17_subcategory_booked,
    coalesce(
        sum(c.total_venue_type_label_consulted), 0
    ) as total_venue_type_label_consulted,
    coalesce(
        sum(
            date_diff(
                u.first_individual_booking_date, u.first_deposit_creation_date, day
            )
        ),
        0
    ) as total_day_between_deposit_and_first_booking,
    coalesce(sum(b.total_diversification), 0) as total_diversification_score,
    coalesce(sum(b.total_distinct_venue_booked), 0) as total_venue_id_booked,
    coalesce(sum(b.total_distinct_venue_type_booked), 0) as total_venue_type_booked,
    coalesce(sum(b.total_distinct_category_booked), 0) as total_category_booked
from users_expired_monthly as u
left join
    consultations as c
    on u.user_id = c.user_id
    and u.deposit_expiration_date = c.deposit_expiration_date
left join
    bookings_info as b
    on u.user_id = b.user_id
    and u.deposit_expiration_date = b.deposit_expiration_date
group by
    date_trunc(u.deposit_expiration_date, month),
    u.total_deposit_amount,
    u.user_is_priority_public,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_is_in_education,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_department_code,
    u.user_department_name,
    u.user_activity,
    u.user_civility,
    u.user_is_theme_subscribed
