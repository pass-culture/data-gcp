-- noqa: disable=all
{{ config(
    cluster_by="active_week",
)
}}
with
    weeks as (
        select *
        from
            unnest(
                generate_date_array('2021-05-17', current_date, interval 1 week)
            ) as week
    ),

    deposit_active_weeks as (
        select
            mrt_global__deposit.user_id,
            user_department_code,
            user_region_name,
            user_birth_date,
            deposit_id,
            deposit_amount,
            deposit_type,
            deposit_reform_category,
            weeks.week as active_week,
            date_trunc(deposit_creation_date, week(monday)) as deposit_creation_week,
            date_diff(
                current_date, deposit_creation_date, week(monday)
            ) as seniority_weeks
        from {{ ref("mrt_global__deposit") }} as mrt_global__deposit
        inner join
            weeks
            on weeks.week
            between date_trunc(
                date(mrt_global__deposit.deposit_creation_date),
                week(monday)
            ) and date_trunc(
                date(mrt_global__deposit.deposit_expiration_date), week(monday)
            )  -- Toutes les semaines de vie du crédit
            and deposit_creation_date > '2021-05-20'  -- Les utilisateurs post sortie de l'app mobile
        inner join
            {{ ref("firebase_aggregated_users") }} as fau
            on mrt_global__deposit.user_id = fau.user_id
            and date(last_connexion_date) >= date(deposit_creation_date)
    ),  -- Uniquement des utilisateurs connectés post octroi du crédit

    aggregated_weekly_deposit_bookings_history as (
        select
            deposit_active_weeks.active_week,
            deposit_active_weeks.deposit_creation_week,
            deposit_active_weeks.user_id,
            deposit_active_weeks.user_department_code,
            deposit_active_weeks.user_region_name,
            if(
                extract(dayofyear from deposit_active_weeks.active_week)
                < extract(dayofyear from deposit_active_weeks.user_birth_date),
                date_diff(
                    deposit_active_weeks.active_week,
                    deposit_active_weeks.user_birth_date,
                    year
                )
                - 1,
                date_diff(
                    deposit_active_weeks.active_week,
                    deposit_active_weeks.user_birth_date,
                    year
                )
            ) as user_age,
            deposit_active_weeks.deposit_id,
            deposit_active_weeks.deposit_type,
            deposit_active_weeks.deposit_reform_category,
            deposit_active_weeks.deposit_amount,
            seniority_weeks,
            date_diff(
                deposit_active_weeks.active_week, deposit_creation_week, week(monday)
            ) as weeks_since_deposit_created,
            date_diff(
                deposit_active_weeks.active_week, deposit_creation_week, month
            ) as months_since_deposit_created,
            coalesce(sum(booking_intermediary_amount), 0) as amount_spent,
            coalesce(count(ebd.booking_id), 0) as cnt_no_cancelled_bookings,
            coalesce(sum(ebd.diversity_score), 0) as delta_diversification
        from deposit_active_weeks
        left join
            {{ ref("mrt_global__booking") }} as ebd
            on deposit_active_weeks.deposit_id = ebd.deposit_id
            and deposit_active_weeks.active_week
            = date_trunc(booking_creation_date, week(monday))
            and not booking_is_cancelled
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),

    cum_booking_history as (
        select
            active_week,
            activity.user_id,
            activity.deposit_id,
            user_department_code,
            user_region_name,
            user_age,
            deposit_creation_week,
            deposit_type,
            deposit_reform_category,
            deposit_amount,
            seniority_weeks,
            weeks_since_deposit_created,
            months_since_deposit_created,
            amount_spent,
            cnt_no_cancelled_bookings,
            coalesce(
                sum(amount_spent) over (
                    partition by activity.deposit_id order by active_week asc
                ),
                0
            ) as cumulative_amount_spent,
            coalesce(
                sum(cnt_no_cancelled_bookings) over (
                    partition by deposit_id order by active_week
                ),
                0
            ) as cumulative_cnt_no_cancelled_bookings,
            coalesce(delta_diversification, 0) as delta_diversification,
            coalesce(
                sum(delta_diversification) over (
                    partition by activity.deposit_id order by active_week
                ),
                0
            ) as delta_diversification_cumsum
        from aggregated_weekly_deposit_bookings_history as activity
    ),

    visits_and_conversion as (
        select
            active_week,
            cum_booking_history.user_id,
            user_department_code,
            user_region_name,
            user_age,
            cum_booking_history.deposit_id,
            deposit_creation_week,
            deposit_type,
            deposit_reform_category,
            deposit_amount,
            seniority_weeks,
            weeks_since_deposit_created,
            months_since_deposit_created,
            amount_spent,
            cnt_no_cancelled_bookings,
            cumulative_amount_spent,
            cumulative_cnt_no_cancelled_bookings,
            delta_diversification,
            delta_diversification_cumsum,
            coalesce(count(distinct session_id), 0) as nb_visits,
            coalesce(
                count(distinct date_trunc(date(first_event_timestamp), day)), 0
            ) as nb_distinct_days_visits,
            coalesce(
                count(
                    distinct
                    case
                        when firebase_session_origin.traffic_campaign is not null
                        then session_id
                    end
                ),
                0
            ) as nb_visits_marketing,
            coalesce(sum(nb_consult_offer), 0) as nb_consult_offer,
            coalesce(sum(nb_booking_confirmation), 0) as nb_booking_confirmation,
            coalesce(sum(nb_add_to_favorites), 0) as nb_add_to_favorites,
            coalesce(sum(visit_duration_seconds), 0) as visit_duration_seconds
        from cum_booking_history
        left join
            {{ ref("firebase_visits") }} as firebase_visits
            on cum_booking_history.user_id = firebase_visits.user_id
            and date_trunc(date(firebase_visits.first_event_timestamp), week(monday))
            = cum_booking_history.active_week
        left join
            {{ ref("firebase_session_origin") }} as firebase_session_origin using (
                user_pseudo_id, session_id
            )
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    ),

    visits_ranked as (
        select
            deposit_id,
            active_week,
            row_number() over (
                partition by deposit_id order by active_week
            ) as visit_rank
        from visits_and_conversion
        where nb_visits > 0
    ),

    only_visitors_since_first_week as (  -- Only keep users with tracking data the week of their credit creation
        select deposit_id
        from visits_and_conversion
        where nb_visits > 0 and weeks_since_deposit_created = 0
    ),

    visits_and_user_engagement_level_metrics as (
        select
            *,
            lag(nb_visits) over (
                partition by deposit_id order by active_week
            ) as visits_previous_week,
            lag(nb_consult_offer) over (
                partition by deposit_id order by active_week
            ) as consult_previous_week,
            count(case when nb_visits > 0 then 1 end) over (
                partition by deposit_id
                order by active_week
                rows between 3 preceding and current row
            ) as nb_co_last_4_weeks,
            count(case when nb_visits > 0 then 1 end) over (
                partition by deposit_id
                order by active_week
                rows between 11 preceding and 8 preceding
            ) as nb_co_3_months_ago,
            count(case when nb_visits > 0 then 1 end) over (
                partition by deposit_id
                order by active_week
                rows between 7 preceding and 4 preceding
            ) as nb_co_2_months_ago,
            count(case when nb_visits > 0 then 1 end) over (
                partition by deposit_id
                order by active_week
                rows between 11 preceding and current row
            ) as nb_co_last_3_months
        from visits_and_conversion
        inner join only_visitors_since_first_week using (deposit_id)
        left join visits_ranked using (deposit_id, active_week)
    ),

    visits_and_user_engagement_level as (
        select
            *,
            case
                when nb_co_last_4_weeks = 4
                then 'Power user'  -- Power user : connected every week for the last 4 weeks
                when
                    nb_co_last_4_weeks > 0
                    and nb_co_3_months_ago > 0
                    and nb_co_2_months_ago > 0
                then 'Core user'  -- Core user : connected at least once every month in the last quarter
                when nb_co_last_3_months > 0
                then 'Casual user'  -- Casual user: connected at least once in the last quarter
                when nb_co_last_3_months = 0
                then 'Dead user'  -- Dead user: no connexion in the last quarter
            end as user_engagement_level,
            case
                when weeks_since_deposit_created <= 4
                then 'New users'  -- Activated last period
                when nb_co_last_4_weeks > 0 and nb_co_2_months_ago > 0
                then 'Current'  -- Active both current and previous period
                when nb_co_2_months_ago > 0 and nb_co_last_4_weeks = 0
                then 'Dormant'  -- Active previous period, not current period
                when nb_co_2_months_ago = 0 and nb_co_last_4_weeks > 0
                then 'Resurrected'  -- Active current period, not previous one
                when nb_co_2_months_ago = 0 and nb_co_last_4_weeks = 0
                then 'Churned'  -- Inactive both current and previous period
            end as user_lifecycle_monthly_state,
            case
                when (weeks_since_deposit_created = 0 or visits_previous_week is null)
                then 'New users'
                when visits_previous_week > 0 and nb_visits > 0
                then 'Current'
                when visits_previous_week > 0 and nb_visits = 0
                then 'Dormant'
                when visits_previous_week = 0 and nb_visits > 0
                then 'Resurrected'
                when visits_previous_week = 0 and nb_visits = 0
                then 'Churned'
            end as user_lifecycle_weekly_state
        from visits_and_user_engagement_level_metrics
    ),

    first_8_weeks_behavior as (
        select
            deposit_id,
            case
                when count(distinct active_week) = 1
                then 'early_churner'
                when count(distinct active_week) <= 3
                then 'yet_to_convince'
                when count(distinct active_week) < 6
                then 'onboarded'
                when count(distinct active_week) >= 6
                then 'early_power_user'
                else 'Autre'
            end as user_first_8_weeks_engagement_level
        from visits_and_user_engagement_level
        where seniority_weeks > 7 and weeks_since_deposit_created <= 7 and nb_visits > 0
        group by 1
    )

select
    visits_and_user_engagement_level.*,
    user_first_8_weeks_engagement_level,
    lag(user_engagement_level) over (
        partition by deposit_id order by active_week
    ) as user_last_week_engagement_level
from visits_and_user_engagement_level
left join first_8_weeks_behavior using (deposit_id)
