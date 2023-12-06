WITH active_dates_geoloc AS (
SELECT 
  DATE_TRUNC(DATE(`{{ bigquery_clean_dataset }}.past_recommended_offers`.date), month) as month_log,
  userid AS user_id,
  user_iris_id AS reco_iris,
  COUNT(DISTINCT `{{ bigquery_clean_dataset }}.past_recommended_offers`.date) nb_log_reco,
FROM  `{{ bigquery_clean_dataset }}.past_recommended_offers` 
WHERE user_iris_id IS NOT NULL 
AND DATE_TRUNC(DATE(date), MONTH) = DATE_TRUNC(DATE('{{ ds }}'), month)
GROUP BY 1, 2, 3 
)

, geoloc_ranked AS (
SELECT 
  *, 
  ROW_NUMBER() OVER(PARTITION BY month_log, user_id ORDER BY nb_log_reco DESC) AS ranking 
FROM active_dates_geoloc
ORDER BY 1, 2, 3, 4)

SELECT 
  month_log, 
  CAST(user_id AS STRING) AS user_id, 
  iriscode AS reco_iris, 
  nb_log_reco,
  iris_france.department
FROM geoloc_ranked 
LEFT JOIN `{{ bigquery_clean_dataset }}.iris_france` iris_france ON geoloc_ranked.reco_iris = iris_france.id
WHERE ranking = 1
