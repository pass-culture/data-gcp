select
    booking.user_id,
    cast(enruser.user_age as int64) as user_age,
    "BOOKING" as event_type,
    booking_creation_date as event_date,
    extract(hour from booking_creation_date) as event_hour,
    extract(dayofweek from booking_creation_date) as event_day,
    extract(month from booking_creation_date) as event_month,
    enroffer.item_id as item_id,
    enroffer.offer_subcategory_id as offer_subcategory_id,
    enroffer.offer_category_id as offer_category_id,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
from `{{ bigquery_raw_dataset }}`.`applicative_database_booking` booking
join
    `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    on enroffer.offer_id = booking.offer_id
inner join
    `{{ bigquery_analytics_dataset }}`.`global_user` enruser
    on enruser.user_id = booking.user_id
where
    booking.booking_creation_date >= date_sub(date("{{ ds }}"), interval 4 month)
    and booking.booking_creation_date <= date("{{ ds }}")
    and booking.user_id is not null
