CREATE TABLE IF NOT EXISTS intermediate.bookings ON CLUSTER default
(
    update_date String,
    offerer_id String,
    offer_id String,
    creation_date String,
    used_date Nullable(String),
    booking_status String,
    deposit_type String,
    booking_quantity UInt64,
    booking_amount Float64
)
ENGINE = MergeTree
PARTITION BY update_date
ORDER BY (offerer_id, booking_status, offer_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Offer bookings on native app, partitioned  by update date ordered by offerer, booking status and offer'