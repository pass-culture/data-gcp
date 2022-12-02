SELECT booking.user_id,
       "BOOKING"                 AS event_type,
       booking_creation_date     AS event_date,
       offer_item_ids.item_id    AS item_id,
       offer.offer_subcategoryid AS offer_subcategoryid,
       subcategories.category_id AS offer_categoryid,
       enroffer.genres,
       enroffer.rayon,
       enroffer.type,
       enroffer.venue_id,
       enroffer.venue_name,
       COUNT(*) AS COUNT,
FROM
    `{{ bigquery_clean_dataset }}`.`applicative_database_booking` booking
    INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_stock` stock
ON booking.stock_id = stock.stock_id
    INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON stock.offer_id = offer.offer_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories ON offer.offer_subcategoryId = subcategories.id
    INNER JOIN `{{ bigquery_sandbox_dataset }}`.`deduplicated_enriched_offer_data` enroffer ON enroffer.offer_id = offer.offer_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids ON offer_item_ids.offer_id = offer.offer_id
WHERE
    booking.booking_creation_date >= DATE_SUB(CURRENT_DATE ()
    , INTERVAL 4 MONTH)
  AND booking.booking_creation_date <= CURRENT_DATE ()
  AND user_id IS NOT NULL
GROUP BY
    booking.user_id,
    item_id,
    event_type,
    booking_creation_date,
    offer_categoryId,
    offer_subcategoryid,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name