
CREATE TABLE IF NOT EXISTS intermediate.offer_consultation ON CLUSTER default
(
    partition_date String,
    offerer_id String,
    offer_id String,
    venue_id String,
    offerer_name Nullable(String),
    venue_name Nullable(String),
    offer_name Nullable(String),
    origin Nullable(String),
    user_role Nullable(String),
    user_age Nullable(Int64),
    cnt_events UInt64
)
ENGINE = MergeTree
PARTITION BY partition_date
ORDER BY (offerer_id,venue_id, offer_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Offer consultations on native app, partitioned by date, offerer, venue and offer'
