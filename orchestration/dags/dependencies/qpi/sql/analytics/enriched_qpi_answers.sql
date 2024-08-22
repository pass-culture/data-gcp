WITH
  qpi_v4 AS (
  SELECT
    user_id,
    submitted_at,
    subcat.category_id,
    subcategories
  FROM
    `{{ bigquery_clean_dataset }}.qpi_answers_v4_clean` uqpi
  JOIN
    `{{ bigquery_analytics_dataset }}.subcategories` subcat
  ON
    subcat.id=uqpi.subcategories )
SELECT
  DISTINCT *
FROM
  qpi_v4
QUALIFY
  RANK() OVER (PARTITION BY user_id ORDER BY submitted_at DESC ) = 1
