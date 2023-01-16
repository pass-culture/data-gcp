SELECT
    *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_date) AS row_index,
    COUNT(*) OVER(PARTITION BY user_id) as total
  FROM
    `{{ bigquery_raw_dataset }}`.`training_data_clicks`
  WHERE
    event_date >= DATE_SUB(DATE("{{ ds }}"), INTERVAL {{ params.event_day_number }} DAY)
)
WHERE row_index < {{ params.train_set_size }} * total
OR total = 1
