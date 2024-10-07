select
    date(event_date) as date,
    case
        when media_source = "bytedanceglobal_int"
        then 'TikTok'
        when media_source = "snapchat_int"
        then 'Snapchat'
        when media_source = "QR_code"
        then 'Custom Url'
        when media_source = "Facebook Ads"
        then 'Facebook'
        when media_source = "restricted"
        then "Unknown"
        else media_source
    end as media_source,
    if(campaign = 'nan', 'Unknown', campaign) as campaign,
    ad,
    adset,
    ad_id,
    event_name,
    appsflyer_id,

from `{{ bigquery_raw_dataset }}.appsflyer_activity_report`
