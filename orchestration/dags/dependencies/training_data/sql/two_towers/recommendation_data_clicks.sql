SELECT DISTINCT
  clicks.user_id,
  clicks.item_id,
  user_features.*,
  item_features.*
FROM
  `{{ bigquery_raw_dataset }}`.training_data_clicks clicks
  JOIN `{{ bigquery_raw_dataset }}`.recommendation_user_features AS user_features ON user_features.user_id = clicks.user_id
  JOIN `{{ bigquery_raw_dataset }}`.recommendation_item_features AS item_features ON item_features.item_id = clicks.item_id