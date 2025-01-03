{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

-- Summarizes cost and installs, picking the latest execution per date
select
    app_id,
    os,
    media_source,
    campaign,
    adset,
    ad,
    cast(date as date) as git,
    cast(execution_date as date) as execution_date,
    cast(sum(cost) as int64) as total_costs,
    sum(installs) as total_installs
from {{ source("raw", "appsflyer_cost_channel") }}
{% if is_incremental() %}
    where
        cast(execution_date as date)
        between date_sub(current_date(), interval 7 day) and current_date()
{% endif %}
group by install_date, execution_date, app_id, os, media_source, campaign, adset, ad
qualify
    row_number() over (
        partition by install_date, app_id, os, media_source, campaign, adset, ad
        order by execution_date desc
    )
    = 1
