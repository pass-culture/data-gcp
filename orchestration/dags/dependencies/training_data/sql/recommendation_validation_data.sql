SELECT user_id, item_id, count
FROM (
  SELECT user_id, item_id, count
  FROM `{{ bigquery_raw_dataset }}`.`recommendation_data`
  WHERE
        (user_id, item_id) NOT IN (
            SELECT (user_id, item_id) FROM `{{ bigquery_raw_dataset }}`.`recommendation_training_data`
        )
    AND item_id IN (
            SELECT DISTINCT item_id FROM `{{ bigquery_raw_dataset }}`.`recommendation_training_data`
        )
    AND RAND() < 0.5
)
