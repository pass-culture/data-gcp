WITH extracted_users AS (  
  SELECT 
    JSON_EXTRACT_SCALAR(event_value, "$.af_firebase_pseudo_id") AS firebase_id, 
    appsflyer_id, 
    advertising_id,
    idfa,
    app_id,
    --
    install_time,
    attributed_touch_time, 
    attributed_touch_type,
    attribution_lookback,
    --
    platform,
    device_type,
    os_version,
    app_version,
    sdk_version,
    
    --
    media_source,
    channel,
    keywords,
    campaign,
    campaign_id,
    adset,
    adset_id,
    ad,
    is_retargeting,
    is_primary_attribution,
    event_time,
    app
  FROM `{{ bigquery_raw_dataset }}.appsflyer_in_app_event_report` 
  WHERE event_name in ("af_open_app")
)

SELECT 
* 
FROM extracted_users
QUALIFY ROW_NUMBER() OVER (PARTITION BY firebase_id ORDER BY install_time ASC ) = 1

