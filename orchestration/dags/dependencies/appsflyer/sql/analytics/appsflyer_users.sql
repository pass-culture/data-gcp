WITH extracted_users AS (  
  SELECT 
    REGEXP_EXTRACT(event_value, r"([0-9]+)") AS user_id,
    appsflyer_id, 
    advertising_id,
    attributed_touch_time, 
    install_time,
    event_time,
    media_source,
    channel,
    keywords,
    campaign,
    campaign_id,
    adset,
    adset_id,
    ad,
    is_retargeting,
    is_primary_attribution
  FROM `{{ bigquery_raw_dataset }}.appsflyer_in_app_event_report` 
  WHERE event_name in ("af_complete_registration", "af_complete_beneficiary_registration")
)

SELECT 
* 
FROM extracted_users
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY install_time ASC ) = 1

