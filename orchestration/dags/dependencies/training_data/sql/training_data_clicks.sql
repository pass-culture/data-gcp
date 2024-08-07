WITH events AS (
    SELECT
        user_id,
        offer_id,
        event_date,
        EXTRACT(HOUR FROM event_timestamp) as event_hour,
        EXTRACT(DAYOFWEEK FROM event_timestamp) as event_day,
        EXTRACT(MONTH FROM event_timestamp) as event_month
    FROM
        `{{ bigquery_int_firebase_dataset }}`.`native_event`
    WHERE
        event_name = "ConsultOffer"
        AND event_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL 6 MONTH)
        AND event_date < DATE("{{ ds }}")
        AND user_id is not null
        AND offer_id is not null
        AND offer_id != 'NaN'
)
SELECT
    events.user_id,
    COALESCE(CAST(enruser.user_age AS INT64), 0) AS user_age,
    "CLICK" as event_type,
    event_date,
    event_hour,
    event_day,
    event_month,
    offer_item_ids.item_id as item_id,
    offer.offer_subcategoryId as offer_subcategoryid,
    subcategories.category_id as offer_categoryId,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name
FROM
    events
    JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer ON offer.offer_id = events.offer_id
    inner join `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories on offer.offer_subcategoryId = subcategories.id
    inner join `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer on enroffer.offer_id = offer.offer_id
    inner join `{{ bigquery_clean_dataset }}`.`offer_item_ids` offer_item_ids on offer_item_ids.offer_id = offer.offer_id
    left join `{{ bigquery_analytics_dataset }}`.`global_user` enruser on enruser.user_id = events.user_id
