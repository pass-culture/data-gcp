qpi_v4 as (
  SELECT 
    user_id
    , submitted_at
    , subcat.category_id
    , subcategories as subcategory_id
FROM `{{ bigquery_clean_dataset }}.qpi_answers_v4` uqpi
join `{{ bigquery_analytics_dataset }}.subcategories` subcat
ON subcat.id=uqpi.subcategories
)
select * from `{{ bigquery_clean_dataset }}.qpi_answers_historical_clean`
UNION ALL
select * from qpi_v4