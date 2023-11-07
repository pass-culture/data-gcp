SELECT 
  user_id,
  iris_france.iriscode,
  emboitements_iris.department
FROM `{{ bigquery_analytics_dataset }}.user_locations`
LEFT JOIN `{{ bigquery_analytics_dataset }}.iris_france` on user_locations.iris_id = iris_france.id
LEFT JOIN `{{ bigquery_clean_dataset }}.emboitements_iris` on iris_france.iriscode = emboitements_iris.code_iris