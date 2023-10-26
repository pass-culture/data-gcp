SELECT 
  date(event_date) as date,
  app,
  CASE 
    WHEN media_source = "bytedanceglobal_int" THEN 'TikTok'
    WHEN media_source = "snapchat_int" THEN 'Snapchat'
    WHEN media_source = "QR_code" THEN 'Custom Url'
    WHEN media_source = "Facebook Ads" THEN 'Facebook'
    WHEN media_source = "restricted" THEN "Unknown"
  ELSE media_source END AS media_source,
  IF(campaign = 'None', 'Unknown', campaign) as campaign,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  sum(ctr) as ctr,
  sum(installs) as installs,
  sum(conversion_rate) as conversion_rate,
  sum(loyal_users) as loyal_users,
  sum(total_cost) as total_cost
 FROM `{{ bigquery_raw_dataset }}.appsflyer_daily_report` 
 WHERE campaign != 'None'
 GROUP BY 1,2,3,4

