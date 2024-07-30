with
weeks as (
    select *
    from
        UNNEST(GENERATE_DATE_ARRAY('2021-05-17', CURRENT_DATE, interval 1 week)) as week
),

deposit_active_weeks as (
    select
        mrt_global__deposit.user_id,
        user_department_code,
        user_region_name,
        user_birth_date,
        deposit_id,
        deposit_amount,
        DATE_TRUNC(deposit_creation_date, week (monday)) as deposit_creation_week,
        deposit_type,
        weeks.week as active_week,
        DATE_DIFF(CURRENT_DATE, deposit_creation_date, week (monday)) as seniority_weeks
    from
        {{ ref('mrt_global__deposit') }} as mrt_global__deposit
        inner join
            weeks
            on
                weeks.week between DATE_TRUNC(DATE(mrt_global__deposit.deposit_creation_date), week (monday))
                and DATE_TRUNC(DATE(mrt_global__deposit.deposit_expiration_date), week (monday)) -- Toutes les semaines de vie du crédit
                and deposit_creation_date > '2021-05-20' -- Les utilisateurs post sortie de l'app mobile
        inner join
            {{ ref('firebase_aggregated_users') }} fau
            on
                mrt_global__deposit.user_id = fau.user_id
                and DATE(last_connexion_date) >= DATE(deposit_creation_date)
), -- Uniquement des utilisateurs connectés post octroi du crédit

aggregated_weekly_deposit_bookings_history as (
    select
        deposit_active_weeks.active_week,
        deposit_active_weeks.deposit_creation_week,
        deposit_active_weeks.user_id,
        deposit_active_weeks.user_department_code,
        deposit_active_weeks.user_region_name,
        IF(EXTRACT(
            dayofyear
            from
            deposit_active_weeks.active_week) < EXTRACT(
            dayofyear
            from
            deposit_active_weeks.user_birth_date
        ), DATE_DIFF(deposit_active_weeks.active_week, deposit_active_weeks.user_birth_date, year
        ) - 1, DATE_DIFF(deposit_active_weeks.active_week, deposit_active_weeks.user_birth_date, year
        )) as user_age,
        deposit_active_weeks.deposit_id,
        deposit_active_weeks.deposit_type,
        deposit_active_weeks.deposit_amount,
        seniority_weeks,
        DATE_DIFF(deposit_active_weeks.active_week, deposit_creation_week, week (monday)) as weeks_since_deposit_created,
        DATE_DIFF(deposit_active_weeks.active_week, deposit_creation_week, month) as months_since_deposit_created,
        COALESCE(SUM(booking_intermediary_amount), 0) as amount_spent,
        COALESCE(COUNT(ebd.booking_id), 0) as cnt_no_cancelled_bookings,
        COALESCE(SUM(delta_diversification), 0) as delta_diversification
    from
        deposit_active_weeks
        left join
            {{ ref('mrt_global__booking') }} ebd
            on
                ebd.deposit_id = deposit_active_weeks.deposit_id
                and deposit_active_weeks.active_week = DATE_TRUNC(booking_creation_date, week (monday))
                and not booking_is_cancelled
        left join
            {{ ref('diversification_booking') }} diversification_booking
            on
                diversification_booking.booking_id = ebd.booking_id
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12
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
        deposit_amount,
        seniority_weeks,
        weeks_since_deposit_created,
        months_since_deposit_created,
        amount_spent,
        cnt_no_cancelled_bookings,
        COALESCE(SUM(amount_spent) over (partition by activity.deposit_id order by active_week asc), 0) as cumulative_amount_spent,
        COALESCE(SUM(cnt_no_cancelled_bookings) over (partition by deposit_id order by active_week), 0) as cumulative_cnt_no_cancelled_bookings,
        COALESCE(delta_diversification, 0) as delta_diversification,
        COALESCE(SUM(delta_diversification) over (partition by activity.deposit_id order by active_week), 0) as delta_diversification_cumsum
    from
        aggregated_weekly_deposit_bookings_history as activity
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
        COALESCE(COUNT(distinct session_id), 0) as nb_visits,
        COALESCE(COUNT(distinct DATE_TRUNC(DATE(first_event_timestamp), day)), 0) as nb_distinct_days_visits,
        COALESCE(COUNT(
            distinct
            case
                when firebase_session_origin.traffic_campaign is not NULL then session_id
                else
                    NULL
            end
        ), 0) as nb_visits_marketing,
        COALESCE(SUM(nb_consult_offer), 0) as nb_consult_offer,
        COALESCE(SUM(nb_booking_confirmation), 0) as nb_booking_confirmation,
        COALESCE(SUM(nb_add_to_favorites), 0) as nb_add_to_favorites,
        COALESCE(SUM(visit_duration_seconds), 0) as visit_duration_seconds
    from
        cum_booking_history
        left join
            {{ ref('firebase_visits') }} firebase_visits
            on
                firebase_visits.user_id = cum_booking_history.user_id
                and DATE_TRUNC(DATE(firebase_visits.first_event_timestamp), week (monday)) = cum_booking_history.active_week
        left join
            {{ ref('firebase_session_origin') }} firebase_session_origin
            using
                (
                    user_pseudo_id,
                    session_id
                )
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18
),

visits_ranked as (
    select
        deposit_id,
        active_week,
        ROW_NUMBER() over (partition by deposit_id order by active_week) as visit_rank
    from
        visits_and_conversion
    where
        nb_visits > 0
),

only_visitors_since_first_week as ( -- Only keep users with tracking data the week of their credit creation
    select deposit_id
    from
        visits_and_conversion
    where
        nb_visits > 0
        and
        weeks_since_deposit_created = 0
),

visits_and_user_engagement_level_metrics as (
    select
        *,
        LAG(nb_visits) over (partition by deposit_id order by active_week) as visits_previous_week,
        LAG(nb_consult_offer) over (partition by deposit_id order by active_week) as consult_previous_week,
        COUNT(case when nb_visits > 0 then 1 else NULL end) over (partition by deposit_id order by active_week rows between 3 preceding and current row) as nb_co_last_4_weeks,
        COUNT(case when nb_visits > 0 then 1 else NULL end) over (partition by deposit_id order by active_week rows between 11 preceding and 8 preceding) as nb_co_3_months_ago,
        COUNT(case when nb_visits > 0 then 1 else NULL end) over (partition by deposit_id order by active_week rows between 7 preceding and 4 preceding) as nb_co_2_months_ago,
        COUNT(case when nb_visits > 0 then 1 else NULL end) over (partition by deposit_id order by active_week rows between 11 preceding and current row) as nb_co_last_3_months
    from
        visits_and_conversion
        inner join
            only_visitors_since_first_week using (deposit_id)
        left join
            visits_ranked
            using
                (
                    deposit_id,
                    active_week
                )
),

visits_and_user_engagement_level as (
    select
        *,
        case
            when nb_co_last_4_weeks = 4 then 'Power user' -- Power user : connected every week for the last 4 weeks
            when nb_co_last_4_weeks > 0 and nb_co_3_months_ago > 0 and nb_co_2_months_ago > 0 then 'Core user' -- Core user : connected at least once every month in the last quarter
            when nb_co_last_3_months > 0 then 'Casual user' -- Casual user: connected at least once in the last quarter
            when nb_co_last_3_months = 0 then 'Dead user' -- Dead user: no connexion in the last quarter
        end as user_engagement_level,
        case
            when weeks_since_deposit_created <= 4 then 'New users' -- Activated last period
            when nb_co_last_4_weeks > 0 and nb_co_2_months_ago > 0 then 'Current' -- Active both current and previous period
            when nb_co_2_months_ago > 0 and nb_co_last_4_weeks = 0 then 'Dormant' -- Active previous period, not current period
            when nb_co_2_months_ago = 0 and nb_co_last_4_weeks > 0 then 'Resurrected' -- Active current period, not previous one
            when nb_co_2_months_ago = 0 and nb_co_last_4_weeks = 0 then 'Churned' -- Inactive both current and previous period
        end as user_lifecycle_monthly_state,
        case
            when (weeks_since_deposit_created = 0 or visits_previous_week is NULL) then 'New users'
            when visits_previous_week > 0 and nb_visits > 0 then 'Current'
            when visits_previous_week > 0 and nb_visits = 0 then 'Dormant'
            when visits_previous_week = 0 and nb_visits > 0 then 'Resurrected'
            when visits_previous_week = 0 and nb_visits = 0 then 'Churned'
        end as user_lifecycle_weekly_state
    from
        visits_and_user_engagement_level_metrics
),

first_8_weeks_behavior as (
    select
        deposit_id,
        case
            when COUNT(distinct active_week) = 1 then 'early_churner'
            when COUNT(distinct active_week) <= 3 then 'yet_to_convince'
            when COUNT(distinct active_week) < 6 then 'onboarded'
            when COUNT(distinct active_week) >= 6 then 'early_power_user'
            else 'Autre'
        end as user_first_8_weeks_engagement_level
    from
        visits_and_user_engagement_level
    where seniority_weeks > 7
        and weeks_since_deposit_created <= 7
        and nb_visits > 0
    group by 1
)

select
    visits_and_user_engagement_level.*,
    user_first_8_weeks_engagement_level,
    LAG(user_engagement_level) over (partition by deposit_id order by active_week) as user_last_week_engagement_level
from
    visits_and_user_engagement_level
    left join first_8_weeks_behavior using (deposit_id)
