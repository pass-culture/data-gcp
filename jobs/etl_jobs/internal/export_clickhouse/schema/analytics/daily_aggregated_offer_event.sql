CREATE OR REPLACE TABLE analytics.daily_aggregated_offer_event ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (venue_id, offer_id, event_date)
    SETTINGS storage_policy = 'gcs_main'
AS
SELECT
    partition_date AS event_date,
    offer_id,
    sum(is_consult_offer) AS offer_consultation_cnt
FROM
    intermediate.native_event
WHERE
    event_name = 'ConsultOffer'
GROUP BY
    venue_id,
    offer_id,
    event_date
