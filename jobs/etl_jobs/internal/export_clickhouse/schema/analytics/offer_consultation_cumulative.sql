CREATE OR REPLACE TABLE analytics.offer_consultation_cumulative ON CLUSTER default
ENGINE = ReplacingMergeTree()
PARTITION BY tuple()
ORDER BY (IFNULL(offer_id, 'unknown_offer_id'), partition_date)
SETTINGS storage_policy = 'gcs_main'
AS
WITH offer_consultations AS (
    SELECT
        partition_date,
        offer_id,
        sum(is_consult_offer) as consultation_cnt
    FROM intermediate.native_event
    WHERE event_name = 'ConsultOffer'
      AND offer_id IS NOT NULL
      AND partition_date >= today() - INTERVAL 6 MONTH
    GROUP BY partition_date, offer_id
)
SELECT
    partition_date,
    offer_id,
    sum(consultation_cnt) OVER (PARTITION BY offer_id ORDER BY partition_date ASC) AS consultation_cumulative_cnt
FROM offer_consultations
