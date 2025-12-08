CREATE OR REPLACE TABLE analytics.daily_aggregated_venue_offer_consultation ON cluster default
    ENGINE = SummingMergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (IFNULL(venue_id, 'unknown_venue_id'), event_date)
    SETTINGS storage_policy = 'gcs_main'
AS
SELECT
    partition_date AS event_date,
    venue_id,
    sum(is_consult_offer) AS consultation_cnt
FROM
    intermediate.native_event
WHERE
    event_name = 'ConsultOffer'
    AND venue_id IS NOT NULL
{% if env_short_name != 'prod' %}
    AND partition_date >= today() - INTERVAL 7 DAY
{% else %}
    AND partition_date >= today() - INTERVAL 6 MONTH
{% endif %}
GROUP BY
    venue_id,
    event_date
