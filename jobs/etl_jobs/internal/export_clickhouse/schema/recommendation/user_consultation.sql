CREATE TABLE IF NOT EXISTS {{ dataset }}.user_consultation(
    partition_date String,
    event_timestamp Datetime64,
    offer_id String, 
    user_id String,
    origin Nullable(String)
)
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY tuple(user_id, offer_id)
  COMMENT 'Users consultations on native app, partitioned by date'
 