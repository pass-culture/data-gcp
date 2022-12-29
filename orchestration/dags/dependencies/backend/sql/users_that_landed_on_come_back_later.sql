
SELECT 
  event_date as event_date,
  user_id as user_id
FROM `{{ bigquery_analytics_dataset }}.firebase_events`
where event_date >= DATE('{{ add_days(ds, -28 )}}')
and firebase_screen = "ComeBackLater"
and event_name = "screen_view"
GROUP BY 1,2
