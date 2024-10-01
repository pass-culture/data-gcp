CREATE OR REPLACE TABLE analytics.daily_aggregated_offer_event ON cluster default
    ENGINE = MergeTree
    PARTITION BY event_date
    ORDER BY (offer_id)
    SETTINGS storage_policy='gcs_main'
AS
SELECT
    toDate(partition_date) AS event_date,
    origin,
    cast(offer_id as String) as offer_id,
    sum(is_consult_offer) AS offer_consultation_cnt
FROM
    intermediate.native_event
WHERE
    is_consult_offer = 1
    AND event_date >= date_sub(current_date(), interval 7 day)
GROUP BY
    event_date,
    origin,
    offer_id
