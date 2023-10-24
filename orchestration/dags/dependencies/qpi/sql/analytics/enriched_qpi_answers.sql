WITH qpi_v4 as (
  SELECT 
    user_id
    , submitted_at
    , subcat.category_id
    , subcategories
FROM `{{ bigquery_clean_dataset }}.qpi_answers_v4_clean` uqpi
join `{{ bigquery_analytics_dataset }}.subcategories` subcat
ON subcat.id=uqpi.subcategories
),

union_all AS (
  SELECT 
    user_id
      , submitted_at
      , category_id
      , subcategory_id as subcategories
  FROM `{{ bigquery_clean_dataset }}.qpi_answers_historical_clean`
  UNION ALL
  SELECT 
    user_id
    , submitted_at
    , category_id
    , subcategories
  FROM qpi_v4
)

SELECT * FROM union_all 
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY submitted_at DESC ) = 1