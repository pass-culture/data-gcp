WITH events AS (
SELECT
user_id,
offer_id,
count(*) as favorites_count,
FROM
    `{{ bigquery_analytics_dataset }}`.`firebase_events`
WHERE event_name = "HasAddedOfferToFavorites"
AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
AND event_date < CURRENT_DATE()
AND user_id is not null
AND offer_id is not null
AND offer_id != 'NaN'
GROUP BY
user_id, offer_id
)
SELECT
user_id,
ANY_VALUE(offer.offer_id) AS offer_id,
ANY_VALUE(offer_subcategoryid) AS offer_subcategoryid,
CASE
WHEN offer.offer_subcategoryId in ('LIVRE_PAPIER','LIVRE_AUDIO_PHYSIQUE','SEANCE_CINE') 
THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id)
END AS item_id,
SUM(favorites_count) as favorites_count,
FROM events
JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON offer.offer_id = events.offer_id
GROUP BY user_id, item_id