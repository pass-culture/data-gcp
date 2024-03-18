CREATE TABLE {{ dataset }}.{{ tmp_table_name }}
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY offer_id AS
  SELECT 
    cast(event_date as String) as partition_date,
    cast(offer_id as String) as offer_id,
    item_id as item_id,
    origin,
    cast(nb_consultation as Nullable(Float64)) as nb_consultation
  FROM gcs(
    gcs_credentials,
  url='{{ bucket_path }}'
  )