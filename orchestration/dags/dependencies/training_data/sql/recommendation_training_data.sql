SELECT
    *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_id, item_id) AS row_index,
    COUNT(*) OVER(PARTITION BY user_id) as total
  FROM
    `raw_stg`.`training_data_bookings`
)
WHERE
    row_index < {{ params.train_set_size }} * total
