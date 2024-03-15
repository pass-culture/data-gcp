CREATE TABLE IF NOT EXISTS {{ dataset }}.venue_offer_statistics(
    update_date String,
    offerer_siren String,
    individual_collective String,
    venue_id String,
    venue_name String,
    venue_public_name String,
    category_id String,
    subcategory String,
    offer_id String,
    offer_name String,
    count_bookings Int64,
    count_used_bookings Int64,
    count_used_tickets_booked Int64,
    count_pending_tickets_booked Int64,
    count_pending_bookings Int64,
    real_amount_booked Float64,
    pending_amount_booked Float64
)
  ENGINE = MergeTree
  COMMENT = 'offer statistics on offerer_siren and venue_id'
  PARTITION BY update_date
  ORDER BY tuple(offerer_siren, venue_id)