
SELECT
    user_pseudo_id
    , session_id
    , MIN(event_date) AS first_event_date
    , MAX(traffic_campaign) AS traffic_campaign
    , MAX(traffic_source) AS traffic_source
    , MAX(traffic_medium) AS traffic_medium
FROM `{{ bigquery_analytics_dataset }}`.firebase_events AS firebase_events
WHERE session_id IS NOT NULL
GROUP BY 1,2