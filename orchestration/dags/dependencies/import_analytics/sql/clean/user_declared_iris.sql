SELECT 
  user_id,
  iris_france.iriscode,
  iris_france.department
FROM `{{ bigquery_analytics_dataset }}.user_locations`
LEFT JOIN `{{ bigquery_analytics_dataset }}.iris_france` on user_locations.iris_id = iris_france.id