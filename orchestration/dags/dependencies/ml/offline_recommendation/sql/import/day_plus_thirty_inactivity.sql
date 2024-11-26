with 
    users_with_last_booking as (
        select
            user_id,
            user_postal_code,
            last_booking_date,
            current_date() as current_date
        from `{{ bigquery_analytics_dataset }}.global_user`
        where date(last_booking_date) = date_sub(current_date(), interval 30 day)
            and user_is_active = true
    ),
    bookings as (
        select
            user_id,
            offer_id,
            booking_created_at as event_date,
            extract(hour from booking_created_at) as event_hour,
            extract(dayofweek from booking_created_at) as event_day,
            extract(month from booking_created_at) as event_month,
            item_id,
            venue_id
        from `{{ bigquery_analytics_dataset }}.global_booking`
        where booking_created_at >= date_sub(current_date(), interval 30 day)
            and user_id is not null
            and offer_id is not null
            and offer_id != 'NaN'
    )
select
    b.user_id,
    u.user_postal_code,
    b.event_date,
    b.event_hour,
    b.event_day,
    b.event_month,
    b.offer_id,
    b.item_id,
    eom.offer_subcategory_id,
    eom.search_group_name,
    evd.venue_latitude,
    evd.venue_longitude
from bookings b
join users_with_last_booking u on b.user_id = u.user_id
join `{{ bigquery_int_applicative_dataset }}.offer_metadata` eom on eom.offer_id = b.offer_id
join `{{ bigquery_analytics_dataset }}.global_venue` evd on evd.venue_id = b.venue_id