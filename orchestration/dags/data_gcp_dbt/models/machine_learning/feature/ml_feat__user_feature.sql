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

    cumulative_bookings as (
        select
            ud.user_id,
            ud.event_date,
            sum(
                case
                    when bd.event_date <= ud.event_date
                    then bd.daily_bookings_count
                    else 0
                end
            ) as user_bookings_count
        from user_days as ud
        left join bookings_by_date as bd on ud.user_id = bd.user_id
        group by ud.user_id, ud.event_date
    )

select
    daily_positions.user_id,
    daily_positions.event_date,
    daily_positions.user_iris_id,
    daily_positions.user_centroid,
    daily_positions.user_centroid_x,
    daily_positions.user_centroid_y,
    cumulative_bookings.user_bookings_count
from cumulative_bookings
left join
    daily_positions
    on cumulative_bookings.user_id = daily_positions.user_id
    and cumulative_bookings.event_date = daily_positions.event_date
order by daily_positions.user_id, daily_positions.event_date
