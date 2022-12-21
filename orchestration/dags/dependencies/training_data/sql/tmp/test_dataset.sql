SELECT user_id, item_id, count 
FROM (
  SELECT user_id, item_id, count FROM `{{ bigquery_raw_dataset }}`.`training_data_clicks`
  WHERE (user_id, item_id) NOT IN (
    SELECT (user_id, item_id) FROM `{{ bigquery_raw_dataset }}`.`training_dataset`
  )
  AND (user_id, item_id) NOT IN (
    SELECT (user_id, item_id) FROM `{{ bigquery_raw_dataset }}`.`evaluation_dataset`
  )
)
