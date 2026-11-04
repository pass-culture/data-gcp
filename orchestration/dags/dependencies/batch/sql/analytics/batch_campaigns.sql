select
    date,
    stats.operating_system,
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
    (
        select campaign_token, operating_system, name
        from `{{ bigquery_raw_dataset }}.batch_campaigns_ref_ios`
        union all
        select campaign_token, operating_system, name
        from `{{ bigquery_raw_dataset }}.batch_campaigns_ref_android`
    ) ref
    on stats.campaign_token = ref.campaign_token
    and stats.operating_system = ref.operating_system
qualify
    row_number() over (
        partition by date, stats.operating_system, stats.campaign_token, stats.version
        order by update_date desc
    )
    = 1
