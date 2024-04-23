CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
  ENGINE = MergeTree
  PARTITION BY update_date
  ORDER BY tuple(offerer_siren, venue_id) 
  SETTINGS storage_policy='gcs_main'  
AS
SELECT 
    cast(update_date as String) as update_date,
    cast(offerer_siren as String) as offerer_siren,
    cast(coalesce(individual_collective, 'INDIVIDUAL') as String) as individual_collective,
    cast(venue_id as String) as venue_id,
    venue_name,
    venue_public_name,
    category_id,
    subcategory,
    cast(coalesce(offer_id, '-1') as String) as offer_id,
    offer_name,
    count_bookings,
    count_used_bookings,
    count_used_tickets_booked,
    count_pending_tickets_booked,
    count_pending_bookings,
    cast(real_amount_booked as Nullable(Float64)) as real_amount_booked,
    cast(pending_amount_booked as Nullable(Float64)) as pending_amount_booked
FROM s3Cluster(
    'default', 
    gcs_credentials,
    url='{{ bucket_path }}'
)
WHERE offerer_siren is not null
AND venue_id is not null
AND offer_id is not null