WITH events AS (
    SELECT
        user_id,
        offer_id,
        event_date,
        count(*) as favorites_count,
    FROM
        `{{ bigquery_analytics_dataset }}`.`firebase_events`
    WHERE
        event_name = "HasAddedOfferToFavorites"
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
        AND event_date < CURRENT_DATE()
        AND user_id is not null
        AND offer_id is not null
        AND offer_id != 'NaN'
    GROUP BY
        user_id,
        offer_id,
        event_date
)
SELECT
    events.user_id,
    CAST(enruser.user_age AS INT64) AS user_age,
    "FAVORITE" as event_type,
    event_date,
    offer_item_ids.item_id as item_id,
    offer.offer_subcategoryId as offer_subcategoryid,
    subcategories.category_id as offer_categoryId,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
    SUM(favorites_count) as count
FROM
    events
    JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON offer.offer_id = events.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories on offer.offer_subcategoryId = subcategories.id
    inner join `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` enroffer on enroffer.offer_id = offer.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids on offer_item_ids.offer_id = offer.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`enriched_user_data` enruser on enruser.user_id = events.user_id
group by
    events.user_id,
    user_age,
    item_id,
    event_type,
    event_date,
    offer_categoryId,
    offer.offer_subcategoryid,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name