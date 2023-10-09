SELECT 
  date(r.event_date) as date, 
  CASE 
    WHEN r.media_source = "bytedanceglobal_int" THEN 'TikTok'
    WHEN r.media_source = "snapchat_int" THEN 'Snapchat'
    WHEN r.media_source = "QR_code" THEN 'Custom Url'
    WHEN r.media_source = "Facebook Ads" THEN 'Facebook'
    WHEN r.media_source = "restricted" THEN "Unknown"
  ELSE r.media_source END AS media_source,
  IF(r.campaign = 'nan', 'Unknown', r.campaign) as campaign,
  r.ad,
  r.adset,
  r.ad_id,
  r.event_name,
  r.appsflyer_id,
  u.firebase_id

FROM `{{ bigquery_raw_dataset }}.appsflyer_activity_report` r
LEFT JOIN `{{ bigquery_analytics_dataset }}.appsflyer_users` u on u.appsflyer_id = r.appsflyer_id


