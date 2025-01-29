-- TODO: deprecated
with
    days as (
        select *
        from
            unnest(
                generate_date_array('2019-02-11', current_date, interval 1 day)
            ) as day
    ),

    user_active_dates as (
        select
            user_id,
            user_department_code,
            user_region_name,
            user_birth_date,
            deposit_id,
            deposit_amount,
            deposit_creation_date,
            deposit_type,
            days.day as active_date,
            date_diff(current_date, deposit_creation_date, day) as seniority_days,
            date_diff(current_date, deposit_creation_date, month) as seniority_months
        from {{ ref("mrt_global__deposit") }} as mrt_global__deposit
        join
            days
            on days.day
            between date(mrt_global__deposit.deposit_creation_date) and date(
                mrt_global__deposit.deposit_expiration_date
            )
    ),

    aggregated_daily_user_used_bookings_history_1 as (
        select
            user_active_dates.active_date,
            user_active_dates.user_id,
            user_active_dates.user_department_code,
            user_active_dates.user_region_name,
            if(
                extract(dayofyear from user_active_dates.active_date)
                < extract(dayofyear from user_active_dates.user_birth_date),
                date_diff(
                    user_active_dates.active_date,
                    user_active_dates.user_birth_date,
                    year
                )
                - 1,
                date_diff(
                    user_active_dates.active_date,
                    user_active_dates.user_birth_date,
                    year
                )
            ) as user_age,
            user_active_dates.deposit_id,
            user_active_dates.deposit_type,
            user_active_dates.deposit_amount as initial_deposit_amount,
            seniority_days,
            seniority_months,
            date_diff(
                user_active_dates.active_date, deposit_creation_date, day
            ) as days_since_deposit_created,
            date_diff(
                user_active_dates.active_date, deposit_creation_date, month
            ) as months_since_deposit_created,
            coalesce(sum(booking_intermediary_amount), 0) as amount_spent,
            count(booking_id) as cnt_used_bookings
        from user_active_dates
        left join
            {{ ref("mrt_global__booking") }} ebd
            on ebd.deposit_id = user_active_dates.deposit_id
            and user_active_dates.active_date = date(booking_used_date)
            and booking_is_used
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    ),

    -- Ajouter score de diversification par jour et score cumulé
    -- /!\ Score de diversification indisponible pour les résas sur les offres
    -- inexistantes dans la table enriched_offer_data
    diversification as (
        select
            divers.user_id,
            divers.booking_id,
            divers.booking_creation_date,
            date(booking_used_date) as booking_used_date,
            delta_diversification
        from {{ ref("diversification_booking") }} divers
        left join
            {{ ref("mrt_global__booking") }} book on divers.booking_id = book.booking_id
    ),

    diversification_used as (
        -- Aggreger le score de diversification selon la date d'utilisation du booking
        select
            user_id,
            booking_used_date,
            count(booking_id) as nb_bookings,
            sum(delta_diversification) as delta_diversification
        from diversification
        group by user_id, booking_used_date
    )

select
    active_date,
    activity.user_id,
    user_department_code,
    user_region_name,
    user_age,
    deposit_id,
    deposit_type,
    initial_deposit_amount,
    seniority_days,
    seniority_months,
    days_since_deposit_created,
    months_since_deposit_created,
    amount_spent,
    cnt_used_bookings,
    sum(amount_spent) over (
        partition by activity.user_id, deposit_id order by active_date asc
    ) as cumulative_amount_spent,
    sum(cnt_used_bookings) over (
        partition by activity.user_id, deposit_id order by active_date
    ) as cumulative_cnt_used_bookings,
    coalesce(delta_diversification, 0) as delta_diversification,
    coalesce(
        sum(delta_diversification) over (
            partition by activity.user_id order by active_date
        ),
        0
    ) as delta_diversification_cumsum
from aggregated_daily_user_used_bookings_history_1 as activity
left join
    diversification_used
    on activity.user_id = diversification_used.user_id
    and activity.active_date = diversification_used.booking_used_date
