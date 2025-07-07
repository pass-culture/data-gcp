CREATE TABLE IF NOT EXISTS intermediate.collective_booking ON CLUSTER default
(
    update_date String CODEC(ZSTD),
    venue_id String CODEC(ZSTD),
    offerer_id String CODEC(ZSTD),
    collective_offer_id String CODEC(ZSTD),
    creation_date String CODEC(ZSTD),
    used_date Nullable(String) CODEC(ZSTD),
    reimbursement_date Nullable(String) CODEC(ZSTD),
    collective_booking_status String CODEC(ZSTD),
    educational_institution_id String CODEC(ZSTD),
    number_of_tickets UInt64 CODEC(T64, ZSTD(3)),
    booking_amount Float64 CODEC(LZ4HC(9))
)
ENGINE = MergeTree
PARTITION BY update_date
ORDER BY (venue_id, offerer_id, collective_booking_status, collective_offer_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Collective offer bookings, partitioned by update date ordered by venue, offerer, booking status and collective offer'
