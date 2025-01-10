{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "acquisition_execution_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

-- Summarizes cost and installs, picking the latest execution per date
select
    app_id,
    os as app_os,
    media_source as acquisition_media_source,
    campaign as acquisition_campaign,
    adset as acquisition_adset,
    ad as acquisition_ad,
    cast(date as date) as app_install_date,
    cast(execution_date as date) as acquisition_execution_date,
    cast(sum(cost) as int64) as total_costs,
    sum(installs) as total_installs
from {{ source("raw", "appsflyer_cost_channel") }}
{% if is_incremental() %}
    where
        cast(execution_date as date)
        between date_sub(date("{{ ds() }}"), interval 7 day) and date("{{ ds() }}")
{% endif %}
group by app_install_date, acquisition_execution_date, app_id, app_os, acquisition_media_source, acquisition_campaign, acquisition_adset, acquisition_ad
qualify
    row_number() over (
        partition by app_install_date, acquisition_execution_date, app_id, app_os, acquisition_media_source, acquisition_campaign, acquisition_adset, acquisition_ad
        order by acquisition_execution_date desc
    )
    = 1
