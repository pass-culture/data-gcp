WITH events AS (
SELECT
user_id,
offer_id,
event_date,
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
user_id, offer_id,event_date
)
SELECT
user_id,
"FAVORITE" as event_type,
event_date,
CASE
    WHEN offer.offer_subcategoryId in (
        'LIVRE_PAPIER',
        'LIVRE_AUDIO_PHYSIQUE',
        'SEANCE_CINE'
    ) THEN CONCAT('product-', offer.offer_product_id)
    ELSE CONCAT('offer-', offer.offer_id)
END AS offer_id,
offer.offer_subcategoryId as offer_subcategoryid,
subcategories.category_id as offer_categoryId,
enroffer.genres,
enroffer.rayon,
enroffer.type,
enroffer.venue_id,
enroffer.venue_name,
SUM(favorites_count) as count,
FROM events
JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON offer.offer_id = events.offer_id
inner join `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories on offer.offer_subcategoryId = subcategories.id
inner join `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` enroffer on enroffer.offer_id = offer.offer_id
group by
    user_id,
    offer_id,
    offer_product_id,
    event_type,
    event_date,
    offer_categoryId,
    offer.offer_subcategoryid,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name