select
    booking.user_id,
    cast(enruser.user_age as int64) as user_age,
    "BOOKING" as event_type,
    booking_creation_date as event_date,
    extract(hour from booking_creation_date) as event_hour,
    extract(dayofweek from booking_creation_date) as event_day,
    extract(month from booking_creation_date) as event_month,
    offer_item_id.item_id as item_id,
    offer.offer_subcategory_id as offer_subcategory_id,
    subcategories.category_id as offer_category_id,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
from `{{ bigquery_raw_dataset }}`.`applicative_database_booking` booking
inner join
    `{{ bigquery_clean_dataset }}`.`applicative_database_stock` stock
    on booking.stock_id = stock.stock_id
inner join
    `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
    on stock.offer_id = offer.offer_id
inner join
    `{{ bigquery_raw_dataset }}`.`subcategories` subcategories
    on offer.offer_subcategory_id = subcategories.id
inner join
    `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    on enroffer.offer_id = offer.offer_id
inner join
    `{{ bigquery_int_applicative_dataset }}`.`offer_item_id` offer_item_id
    on offer_item_id.offer_id = offer.offer_id
inner join
    `{{ bigquery_analytics_dataset }}`.`global_user` enruser
    on enruser.user_id = booking.user_id
where
    booking.booking_creation_date >= date_sub(date("{{ ds }}"), interval 4 month)
    and booking.booking_creation_date <= date("{{ ds }}")
    and booking.user_id is not null
