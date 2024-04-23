CREATE TABLE IF NOT EXISTS intermediate.user_consultation ON cluster default(
    partition_date String,
    event_timestamp Datetime64,
    offer_id String, 
    user_id String,
    origin Nullable(String)
)
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY tuple(user_id, offer_id)
  SETTINGS storage_policy='gcs_main'
  COMMENT 'Users consultations on native app, partitioned by date'
 