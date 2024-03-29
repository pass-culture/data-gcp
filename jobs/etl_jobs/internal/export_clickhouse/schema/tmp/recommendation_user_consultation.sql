CREATE TABLE {{ dataset }}.{{ tmp_table_name }}
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY tuple(user_id, offer_id) AS
  SELECT 
    cast(event_date as String) as partition_date,
    cast(event_timestamp as Datetime64) as event_timestamp,
    cast(offer_id as String) as offer_id,
    cast(user_id as String) as user_id,
    origin
  FROM gcs(
    gcs_credentials,
  url='{{ bucket_path }}'
  )