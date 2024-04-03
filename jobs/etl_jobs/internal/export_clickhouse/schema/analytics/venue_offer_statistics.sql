CREATE TABLE IF NOT EXISTS {{ dataset }}.venue_offer_statistics(
    update_date String,
    offerer_siren String,
    individual_collective String,
    venue_id String,
    venue_name Nullable(String),
    venue_public_name Nullable(String),
    category_id Nullable(String),
    subcategory Nullable(String),
    offer_id String,
    offer_name Nullable(String),
    count_bookings Nullable(Int64),
    count_used_bookings Nullable(Int64),
    count_used_tickets_booked Nullable(Int64),
    count_pending_tickets_booked Nullable(Int64),
    count_pending_bookings Nullable(Int64),
    real_amount_booked Nullable(Float64),
    pending_amount_booked Nullable(Float64)
)
  ENGINE = MergeTree
  PARTITION BY update_date
  ORDER BY tuple(offerer_siren, venue_id)
  COMMENT 'Venue offer individual and collective statistics'
