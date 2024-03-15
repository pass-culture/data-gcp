CREATE TABLE {{ dataset }}.tmp_venue_offer_statistics_{{ date }}
  ENGINE = MergeTree
  PARTITION BY update_date
  ORDER BY tuple(offerer_siren, venue_id) AS
SELECT 
    update_date,
    cast(offerer_siren as String) as offerer_siren,
    cast(individual_collective as String) as individual_collective,
    cast(venue_id as String) as venue_id,
    cast(venue_name as String) as venue_name,
    cast(venue_public_name as String) as venue_public_name,
    cast(category_id as String) as category_id,
    cast(subcategory as String) as subcategory,
    cast(offer_id as String) as offer_id,
    cast(offer_name as String) as offer_name,
    count_bookings,
    count_used_bookings,
    count_used_tickets_booked,
    count_pending_tickets_booked,
    count_pending_bookings,
    real_amount_booked,
    pending_amount_booked
FROM gcs(
    gcs_credentials,
    url='{{ bucket_path }}'
)