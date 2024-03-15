CREATE TABLE IF NOT EXISTS {{ dataset }}.offer_consultation(
    partition_date String,
    offer_id String, 
    item_id Nullable(String),
    origin String, 
    nb_consultation Float64
)
  ENGINE = MergeTree
  COMMENT = 'Daily statistics on offer consultation'
  PARTITION BY partition_date
  ORDER BY offer_id
 