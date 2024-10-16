CREATE TABLE IF NOT EXISTS intermediate.collective_booking ON CLUSTER default
(
    update_date String,
    venue_id String,
    offerer_id String,
    collective_offer_id String,
    offer_id Nullable(String),
    creation_date String,
    used_date Nullable(String),
    reimbursement_date Nullable(String),
    collective_booking_status String,
    educational_institution_id String,
    number_of_tickets UInt64,
    booking_amount Float64
)
ENGINE = MergeTree
PARTITION BY update_date
ORDER BY (venue_id, offerer_id, collective_booking_status, collective_offer_id)
SETTINGS storage_policy='gcs_main'
COMMENT 'Collective offer bookings, partitioned by update date ordered by venue, offerer, booking status and collective offer'
