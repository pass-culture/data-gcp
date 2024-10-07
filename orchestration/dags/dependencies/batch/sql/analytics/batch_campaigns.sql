select
    date,
    operating_system,
    stats.campaign_token,
    name as campaign_name,
    stats.version,
    sent,
    sent_optins,
    direct_open,
    influenced_open,
    reengaged,
    errors,
    update_date
from `{{ bigquery_raw_dataset }}.batch_campaigns_stats` stats
left join
    `{{ bigquery_raw_dataset }}.batch_campaigns_ref` ref
    on stats.campaign_token = ref.campaign_token
qualify
    row_number() over (
        partition by date, operating_system, campaign_token, stats.version
        order by update_date desc
    )
    = 1
