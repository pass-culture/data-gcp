CREATE TABLE IF NOT EXISTS intermediate.venue_offer_statistic ON CLUSTER default
(
    update_date String CODEC(ZSTD),
    venue_id String CODEC(ZSTD),
    total_active_offers UInt64 CODEC(T64, ZSTD(3)),
    total_pending_offers UInt64 CODEC(T64, ZSTD(3)),
    total_inactive_non_rejected_offers UInt64 CODEC(T64, ZSTD(3)),
    total_active_collective_offers UInt64 CODEC(T64, ZSTD(3)),
    total_pending_collective_offers UInt64 CODEC(T64, ZSTD(3)),
    total_inactive_non_rejected_collective_offers UInt64 CODEC(T64, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (venue_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Offer statistics, ordered by venue id'
