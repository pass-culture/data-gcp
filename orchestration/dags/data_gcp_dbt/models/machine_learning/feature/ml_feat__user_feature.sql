{{
    config(
        materialized="table",
    )
}}

with
    bookings_raw as (
        select user_id, booking_creation_date as event_date
        from {{ ref("int_applicative__booking") }}
        where not booking_is_cancelled
    ),

    favorites_raw as (
        select user_id, favorite_creation_date as event_date
        from {{ ref("int_applicative__favorite") }}
    ),

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

    date_series as (
        select distinct event_date from daily_positions order by event_date
    ),

    distinct_users as (
        select distinct user_id
        from bookings_raw
        union distinct
        select distinct user_id
        from daily_positions
    ),

    user_days as (
        select users.user_id, dates.event_date
        from distinct_users as users
        cross join date_series as dates
    ),

    bookings_by_date as (
        select user_id, event_date, count(*) as daily_bookings_count
        from bookings_raw
        group by user_id, event_date
    ),

    favorites_by_date as (
        select user_id, event_date, count(*) as daily_favorites_count
        from favorites_raw
        group by user_id, event_date
    ),

    cumulative_bookings as (
        select
            user_days.user_id,
            user_days.event_date,
            sum(
                case
                    when bookings_by_date.event_date <= user_days.event_date
                    then bookings_by_date.daily_bookings_count
                    else 0
                end
            ) as user_bookings_count
        from user_days
        left join bookings_by_date on user_days.user_id = bookings_by_date.user_id
        group by user_days.user_id, user_days.event_date
    ),

    cumulative_favorites as (
        select
            user_days.user_id,
            user_days.event_date,
            sum(
                case
                    when favorites_by_date.event_date <= user_days.event_date
                    then favorites_by_date.daily_favorites_count
                    else 0
                end
            ) as user_favorites_count
        from user_days
        left join favorites_by_date on user_days.user_id = favorites_by_date.user_id
        group by user_days.user_id, user_days.event_date
    )

select
    daily_positions.user_id,
    daily_positions.event_date,
    daily_positions.user_iris_id,
    daily_positions.user_centroid,
    daily_positions.user_centroid_x,
    daily_positions.user_centroid_y,
    cumulative_bookings.user_bookings_count,
    cumulative_favorites.user_favorites_count
from cumulative_bookings
left join
    daily_positions
    on cumulative_bookings.user_id = daily_positions.user_id
    and cumulative_bookings.event_date = daily_positions.event_date
left join
    cumulative_favorites
    on daily_positions.user_id = cumulative_favorites.user_id
    and daily_positions.event_date = cumulative_favorites.event_date
order by daily_positions.user_id, daily_positions.event_date
