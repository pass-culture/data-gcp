SELECT
    booking.user_id,
    "BOOKING" as event_type,
    offer_item_ids.item_id as item_id,
    offer.offer_subcategoryId as offer_subcategoryId,
    subcategories.category_id as offer_categoryId,
    COUNT(*) as count
from
    `{{ bigquery_clean_dataset }}`.`applicative_database_booking` booking
    INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_stock` stock ON booking.stock_id = stock.stock_id
    INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON stock.offer_id = offer.offer_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories ON offer.offer_subcategoryId = subcategories.id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids ON offer_item_ids.offer_id = offer.offer_id
WHERE
    booking.booking_creation_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 4 MONTH)
    AND booking.booking_creation_date <= DATE("{{ ds }}")
    AND user_id IS NOT NULL
GROUP BY
    booking.user_id,
    item_id,
    event_type,
    offer_categoryId,
    offer_subcategoryId
