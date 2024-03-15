CREATE TABLE {{ dataset }}.tmp_offer_consultation_{{ date }}
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY offer_id AS
  SELECT 
    event_date as partition_date,
    cast(offer_id as String) as offer_id,
    item_id as item_id,
    origin
    cast(nb_consultation as Float64) as nb_consultation
  FROM gcs(
    gcs_credentials,
  url='{{ bucket_path }}'
  )