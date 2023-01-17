SELECT
  *
FROM `{{ bigquery_raw_dataset }}`.`training_data_clicks`
WHERE
  event_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL {{ params.event_day_number }} DAY)
AND user_id IN (
  SELECT DISTINCT user_id FROM `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_training_data_clicks`
)
AND item_id IN (
  SELECT DISTINCT item_id FROM `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_training_data_clicks`
)
AND (user_id, item_id) NOT IN (
  SELECT (user_id, item_id) FROM `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_training_data_clicks`
)
AND (user_id, item_id) NOT IN (
  SELECT (user_id, item_id) FROM `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_validation_data_clicks`
)
