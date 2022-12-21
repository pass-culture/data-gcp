SELECT
    user_id,
    item_id,
    count
FROM (
  SELECT
    user_id,
    item_id,
    SUM(count) AS count,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY RAND()) AS row_index,
    COUNT(*) OVER(PARTITION BY user_id) as total
  FROM
    `{{ bigquery_raw_dataset }}`.`training_data_clicks`
  GROUP BY
    user_id,
    item_id
)
WHERE
    row_index < 0.8 * total
