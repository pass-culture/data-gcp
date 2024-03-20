SELECT 
  date(event_date) as date, 
  CASE 
    WHEN media_source = "bytedanceglobal_int" THEN 'TikTok'
    WHEN media_source = "snapchat_int" THEN 'Snapchat'
    WHEN media_source = "QR_code" THEN 'Custom Url'
    WHEN media_source = "Facebook Ads" THEN 'Facebook'
    WHEN media_source = "restricted" THEN "Unknown"
  ELSE media_source END AS media_source,
  IF(campaign = 'nan', 'Unknown', campaign) as campaign,
  ad,
  adset,
  ad_id,
  event_name,
  appsflyer_id,

FROM `{{ bigquery_raw_dataset }}.appsflyer_activity_report` 

