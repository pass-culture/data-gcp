WITH events AS (
    SELECT
        user_id,
        offer_id,
        count(*) AS clicks_count,
    FROM
        `{{ bigquery_analytics_dataset }}`.`firebase_events`
    WHERE
        event_name = "ConsultOffer"
        AND event_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 4 MONTH)
        AND event_date < DATE("{{ ds }}")
        AND user_id IS NOT NULL
        AND offer_id IS NOT NULL
        AND offer_id != 'NaN'
    GROUP BY
        user_id,
        offer_id,
)
SELECT
    user_id,
    offer_item_ids.item_id AS item_id,
    offer.offer_subcategoryId AS offer_subcategoryId,
    subcategories.category_id AS offer_categoryId,
    SUM(clicks_count) AS count,
FROM
    events
    JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON offer.offer_id = events.offer_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories ON offer.offer_subcategoryId = subcategories.id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` enroffer ON enroffer.offer_id = offer.offer_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids ON offer_item_ids.offer_id = offer.offer_id
GROUP BY
    user_id,
    item_id, 
    event_type,
    offer_categoryId,
    offer.offer_subcategoryid
