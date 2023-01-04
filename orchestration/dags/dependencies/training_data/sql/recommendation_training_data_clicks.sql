SELECT
    *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_id, item_id) AS row_index,
    COUNT(*) OVER(PARTITION BY user_id) as total
  FROM
    `{{ bigquery_raw_dataset }}`.`training_data_clicks`
)
WHERE row_index < {{ params.train_set_size }} * total
OR total = 1
