CREATE OR REPLACE TABLE analytics.last_3months_venue_top_offer_consultation ON CLUSTER default
ENGINE = ReplacingMergeTree()
PARTITION BY tuple()
ORDER BY (IFNULL(venue_id, 'unknown_venue_id'), rank)
SETTINGS storage_policy = 'gcs_main'
AS
WITH offer_consultations_3m AS (
    SELECT
        venue_id,
        offer_id,
        sum(is_consult_offer) as consultation_cnt
    FROM intermediate.native_event
    WHERE event_name = 'ConsultOffer'
      AND venue_id IS NOT NULL
      AND offer_id IS NOT NULL
      AND partition_date >= today() - INTERVAL 3 MONTH
    GROUP BY venue_id, offer_id
),
ranked_offers AS (
    SELECT
        venue_id,
        offer_id,
        consultation_cnt,
        row_number() OVER (
            PARTITION BY venue_id
            ORDER BY consultation_cnt DESC
        ) as rank
    FROM offer_consultations_3m
)
SELECT
    venue_id,
    offer_id,
    consultation_cnt,
    rank
FROM ranked_offers
WHERE rank <= 3
