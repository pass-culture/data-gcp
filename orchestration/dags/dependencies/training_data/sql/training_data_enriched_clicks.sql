SELECT DISTINCT
  clicks.event_date,
  clicks.user_id,
  clicks.item_id,
  clicks.event_hour,
  clicks.event_day,
  clicks.event_month,
  * EXCEPT (event_date, user_id, item_id, event_hour, event_day, event_month)
FROM
  (SELECT event_date, user_id, item_id,event_hour,event_day,event_month FROM `{{ bigquery_raw_dataset }}`.training_data_clicks) AS clicks
  LEFT JOIN `{{ bigquery_raw_dataset }}`.recommendation_user_features AS user_features ON user_features.user_id = clicks.user_id
  LEFT JOIN `{{ bigquery_raw_dataset }}`.recommendation_item_features AS item_features ON item_features.item_id = clicks.item_id
