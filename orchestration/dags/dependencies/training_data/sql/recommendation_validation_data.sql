SELECT
  *
FROM `{{ bigquery_raw_dataset }}`.`training_data_bookings`
WHERE user_id IN (
  SELECT DISTINCT user_id FROM `{{ bigquery_raw_dataset }}`.`recommendation_training_data`
)
AND item_id IN (
  SELECT DISTINCT item_id FROM `{{ bigquery_raw_dataset }}`.`recommendation_training_data`
)
AND (user_id, item_id) NOT IN (
  SELECT (user_id, item_id) FROM `{{ bigquery_raw_dataset }}`.`recommendation_training_data`
)
AND MOD(ABS(FARM_FINGERPRINT(CONCAT(user_id, item_id))), 2) < 1
