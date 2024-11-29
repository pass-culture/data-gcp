CREATE TABLE IF NOT EXISTS intermediate.booking ON CLUSTER default
(
    update_date String,
    venue_id String,
    offerer_id String,
    offer_id String,
    creation_date String,
    used_date Nullable(String),
    reimbursement_date Nullable(String),
    stock_beginning_date Nullable(String),
    booking_status String,
    deposit_type String,
    booking_quantity UInt64,
    booking_amount Float64
)
ENGINE = MergeTree
PARTITION BY update_date
ORDER BY (venue_id, offerer_id, booking_status, offer_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Offer bookings on native app, partitioned by update date ordered by venue, offerer, booking status and offer'
