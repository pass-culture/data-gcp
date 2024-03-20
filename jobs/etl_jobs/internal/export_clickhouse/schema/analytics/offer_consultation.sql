CREATE TABLE IF NOT EXISTS {{ dataset }}.offer_consultation(
    partition_date String,
    offer_id String, 
    item_id Nullable(String),
    origin Nullable(String), 
    nb_consultation Nullable(Float64)
)
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY offer_id
  COMMENT 'Daily statistics on offer consultation'