CREATE TABLE IF NOT EXISTS {{ dataset }}.user_clicks(
    partition_date String,
    event_timestamp Timestamp,
    offer_id String, 
    user_id String,
    origin Nullable(String)
)
  ENGINE = MergeTree
  COMMENT = 'raw table with users clicks on the app, partitioned by date'
  PARTITION BY partition_date
  ORDER BY tuple(user_id, offer_id) AS
 