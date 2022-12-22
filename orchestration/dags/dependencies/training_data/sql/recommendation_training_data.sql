SELECT
    *
FROM (
  SELECT
    user_id,
    item_id,
    offer_subcategoryId,
    offer_categoryId,
    SUM(count) AS count,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_id, item_id) AS row_index,
    COUNT(*) OVER(PARTITION BY user_id) as total
  FROM
    `{{ bigquery_raw_dataset }}`.`recommendation_data`
  GROUP BY
    user_id,
    item_id,
    offer_subcategoryId,
    offer_categoryId
)
WHERE
    row_index < {{ params.train_set_size }} * total
