SELECT
    booking.user_id,
    enruser.user_age,
    "BOOKING" as event_type,
    booking_creation_date as event_date,
    offer_item_ids.item_id as item_id,
    offer.offer_subcategoryId as offer_subcategoryid,
    subcategories.category_id as offer_categoryId,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
    count(*) as count,
from
    `{{ bigquery_clean_dataset }}`.`applicative_database_booking` booking
    inner join `{{ bigquery_clean_dataset }}`.`applicative_database_stock` stock on booking.stock_id = stock.stock_id
    inner join `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer on stock.offer_id = offer.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories on offer.offer_subcategoryId = subcategories.id
    inner join `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` enroffer on enroffer.offer_id = offer.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids on offer_item_ids.offer_id = offer.offer_id
    left join `{{ bigquery_analytics_dataset }}`.`enriched_user_data` enruser on enruser.user_id = events.user_id
where
    booking.booking_creation_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 4 MONTH)
    and booking.booking_creation_date <= DATE("{{ ds }}")
    and user_id is not null
group by
    booking.user_id,
    item_id,
    event_type,
    enruser.user_age,
    booking_creation_date,
    offer_categoryId,
    offer_subcategoryid,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name