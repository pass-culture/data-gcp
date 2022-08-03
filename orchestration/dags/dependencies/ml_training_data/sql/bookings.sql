SELECT 
booking.user_id,
"BOOKING" as event_type,
booking_creation_date as event_date,
(CASE WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE') 
    THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) 
END) AS offer_id,
offer.offer_subcategoryId as offer_subcategoryid,
subcategories.category_id as offer_categoryId,
enroffer.genres, 
enroffer.rayon, 
enroffer.type, 
enroffer.venue_id, 
enroffer.venue_name,
count(*) as count,
from `{{ bigquery_clean_dataset }}`.`applicative_database_booking` booking

inner join `{{ bigquery_clean_dataset }}`.`applicative_database_stock` stock
on booking.stock_id = stock.stock_id

inner join `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
on stock.offer_id = offer.offer_id

inner join `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories
on offer.offer_subcategoryId = subcategories.id

inner join `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` enroffer
on enroffer.offer_id = offer.offer_id

where booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
and booking.booking_creation_date <= CURRENT_DATE()
and user_id is not null
group by booking.user_id, offer.offer_id, offer_product_id,event_type,booking_creation_date,offer_categoryId, offer_subcategoryid,enroffer.genres, enroffer.rayon, enroffer.type, enroffer.venue_id, enroffer.venue_name