SELECT DISTINCT
  book.event_date,
  book.event_type,
  book.user_id,
  book.item_id,
  book.count,
  * EXCEPT (event_date, user_id, item_id, count)
FROM
  (SELECT event_date, user_id, item_id, event_type, count FROM `{{ bigquery_raw_dataset }}`.training_data_bookings) AS book
  LEFT JOIN `{{ bigquery_raw_dataset }}`.recommendation_user_features AS user_features ON user_features.user_id = book.user_id
  LEFT JOIN `{{ bigquery_raw_dataset }}`.recommendation_item_features AS item_features ON item_features.item_id = book.item_id
