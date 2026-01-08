CREATE OR REPLACE TABLE analytics.last_30_day_venue_top_offer_consultation ON CLUSTER default
ENGINE = ReplacingMergeTree()
PARTITION BY tuple()
ORDER BY (IFNULL(venue_id, 'unknown_venue_id'), rank)
SETTINGS storage_policy = 'gcs_main'
AS
WITH offer_consultations_30d AS (
    SELECT
        venue_id,
        offer_id,
        sum(is_consult_offer) as consultation_cnt
    FROM intermediate.native_event
    WHERE event_name = 'ConsultOffer'
      AND venue_id IS NOT NULL
      AND offer_id IS NOT NULL
      AND partition_date >= today() - INTERVAL 30 DAY
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
    FROM offer_consultations_30d
)
SELECT
    venue_id,
    offer_id,
    consultation_cnt,
    rank
FROM ranked_offers
WHERE rank <= 3
