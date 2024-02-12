{{
  config(
    materialized='incremental',
    partition_by={
      'field': 'month_log',
      'data_type': 'date'
      },
    incremental_strategy='insert_overwrite'
  )
}}


WITH active_dates_geoloc AS (
SELECT 
  DATE_TRUNC(pro.event_date, month) as month_log,
  user_id,
  user_iris_id AS reco_iris,
  COUNT(DISTINCT pro.event_date) as nb_log_reco,
FROM {{ source('analytics', 'firebase_recommendation_events') }} pro
WHERE user_iris_id IS NOT NULL 
{% if is_incremental() %}
-- recalculate latest day's data + previous
AND DATE_TRUNC(DATE(event_date), MONTH) = DATE_TRUNC(DATE('2023-12-01'), month)
{% endif %}
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
LEFT JOIN {{ source('clean', 'iris_france') }} iris_france ON geoloc_ranked.reco_iris = iris_france.id
WHERE ranking = 1
