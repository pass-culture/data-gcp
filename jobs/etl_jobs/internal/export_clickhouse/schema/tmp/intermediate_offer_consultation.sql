
CREATE TABLE {{ dataset }}.{{ tmp_table_name }}
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY (offerer_id,venue_id, offer_id)
  SETTINGS storage_policy='gcs_main'  
AS
  SELECT 
    cast(event_date as String) as partition_date,
    cast(offerer_id as String) as offerer_id,
    cast(offer_id as String) as offer_id,
    cast(venue_id as String) as venue_id,
    offerer_name,
    venue_name,
    offer_name,
    origin,
    user_role,
    user_age,
    cast(cnt_events as UInt64) as cnt_events
  FROM s3Cluster(
    'default', 
    gcs_credentials,
    url='{{ bucket_path }}'
)
