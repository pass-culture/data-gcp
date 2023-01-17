SELECT
  *
FROM
  `{{ bigquery_analytics_dataset }}`.firebase_aggregated_users firebase
  JOIN `{{ bigquery_analytics_dataset }}`.enriched_user_data enriched ON firebase.user_id = enriched.user_id
