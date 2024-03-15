CREATE TABLE {{ dataset }}.tmp_offer_consultation_{{ date }}
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY tuple(user_id, offer_id) AS
  SELECT 
    event_date as partition_date,
    cas(event_timestamp as Timestamp) as event_timestamp,
    cast(offer_id as String) as offer_id,
    cast(user_id as String) as user_id,
    origin
  FROM gcs(
    gcs_credentials,
  url='{{ bucket_path }}'
  )