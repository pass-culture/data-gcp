CREATE TABLE IF NOT EXISTS intermediate.booking ON CLUSTER default
(
    update_date String CODEC(ZSTD),
    venue_id String CODEC(ZSTD),
    offerer_id String CODEC(ZSTD),
    offer_id String CODEC(ZSTD),
    creation_date String CODEC(ZSTD),
    used_date Nullable(String) CODEC(ZSTD),
    booking_status String CODEC(ZSTD),
    deposit_type String CODEC(ZSTD),
    booking_quantity UInt64 CODEC(T64, ZSTD(3)),
    booking_amount Float64 CODEC(LZ4HC(9))
)
ENGINE = MergeTree
PARTITION BY update_date
ORDER BY (venue_id, offerer_id, booking_status, offer_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Offer bookings on native app, partitioned by update date ordered by venue, offerer, booking status and offer'
