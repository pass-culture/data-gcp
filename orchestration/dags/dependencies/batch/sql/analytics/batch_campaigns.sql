SELECT 
    date
    , operating_system
    , stats.campaign_token
    , name as campaign_name
    , version
    , sent
    , sent_optins
    , direct_open
    , influenced_open
    , reengaged
    , errors
    , update_date
FROM `{{ bigquery_raw_dataset }}.batch_campaigns_stats` stats
LEFT JOIN `{{ bigquery_raw_dataset }}.batch_campaigns_ref` ref
ON stats.campaign_token = ref.campaign_token
QUALIFY row_number() over(partition by date, operating_system, campaign_token, version order by update_date desc) = 1