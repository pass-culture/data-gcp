SELECT
    booking.user_id,
    CAST(enruser.user_age AS INT64) AS user_age,
    "BOOKING" as event_type,
    booking_creation_date as event_date,
    EXTRACT(HOUR FROM booking_creation_date) as event_hour,
    EXTRACT(DAYOFWEEK FROM booking_creation_date) as event_day,
    EXTRACT(MONTH FROM booking_creation_date) as event_month,
    offer_item_ids.item_id as item_id,
    offer.offer_subcategoryId as offer_subcategoryid,
    subcategories.category_id as offer_categoryId,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
from
    `{{ bigquery_clean_dataset }}`.`booking` booking
    inner join `{{ bigquery_clean_dataset }}`.`applicative_database_stock` stock on booking.stock_id = stock.stock_id
    inner join `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer on stock.offer_id = offer.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories on offer.offer_subcategoryId = subcategories.id
    inner join `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` enroffer on enroffer.offer_id = offer.offer_id
    inner join `{{ bigquery_clean_dataset }}`.`offer_item_ids` offer_item_ids on offer_item_ids.offer_id = offer.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`enriched_user_data` enruser on enruser.user_id = booking.user_id
where
    booking.booking_creation_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 4 MONTH)
    and booking.booking_creation_date <= DATE("{{ ds }}")
    and booking.user_id is not null