CREATE OR REPLACE TABLE analytics.daily_aggregated_offer_event ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (offer_id, event_date)
    SETTINGS storage_policy = 'gcs_main'
AS
SELECT
    toDate(partition_date) AS event_date,
    cast(offer_id as String) as offer_id,
    sum(is_consult_offer) AS offer_consultation_cnt
FROM
    intermediate.native_event
WHERE
    is_consult_offer = 1
GROUP BY
    offer_id,
    event_date
