{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

with

    daily_positions as (
        select
            user_id,
            event_date,
            user_iris_id,
            user_centroid,
            user_centroid_x,
            user_centroid_y
        from {{ ref("ml_feat__user_daily_iris_location") }}
    ),

    aggregated_weekly_user_data as (
        select
            user_id,
            active_week,
            nb_consult_offer as user_clicks_count,
            nb_booking_confirmation as user_bookings_count,
            nb_add_to_favorites as user_favorites_count,
            delta_diversity_cumsum as user_diversity_count,
            deposit_amount as user_deposit_amount,
            cumulative_amount_spent as user_amount_spent
        from {{ ref("aggregated_weekly_user_data") }}
    ),

    date_series as (
        select distinct event_date from daily_positions order by event_date
    ),

    distinct_users as (
        select distinct user_id
        from aggregated_weekly_user_data
        union distinct
        select distinct user_id
        from daily_positions
    ),

    user_days as (
        select
            users.user_id,
            dates.event_date,
            date_trunc(dates.event_date, week(monday)) as active_week
        from distinct_users as users
        cross join date_series as dates
    )

select
    user_days.user_id,
    user_days.event_date,
    user_days.active_week,
    daily_positions.user_iris_id,
    daily_positions.user_centroid,
    daily_positions.user_centroid_x,
    daily_positions.user_centroid_y,
    aggregated_weekly_user_data.user_bookings_count,
    aggregated_weekly_user_data.user_clicks_count,
    aggregated_weekly_user_data.user_favorites_count,
    aggregated_weekly_user_data.user_diversity_count,
    aggregated_weekly_user_data.user_deposit_amount,
    aggregated_weekly_user_data.user_amount_spent
from user_days
left join
    daily_positions
    on user_days.user_id = daily_positions.user_id
    and user_days.event_date = daily_positions.event_date
left join
    aggregated_weekly_user_data
    on user_days.user_id = aggregated_weekly_user_data.user_id
    and user_days.active_week = aggregated_weekly_user_data.active_week
order by user_days.user_id, user_days.event_date
