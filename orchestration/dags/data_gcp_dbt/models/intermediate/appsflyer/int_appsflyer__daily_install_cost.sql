{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "acquisition_execution_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

with

    -- Summarizes campaign cost and installs, picking the latest execution per date
    campaign_installs as (
        select
            os as app_os,
            media_source as acquisition_media_source,
            if(campaign = '', 'None', campaign) as acquisition_campaign,
            if(adset = '', 'None', adset) as acquisition_adset,
            if(ad = '', 'None', ad) as acquisition_ad,
            date(date) as app_install_date,
            date(execution_date) as acquisition_execution_date,
            safe_cast(sum(cost) as int64) as total_costs,
            sum(installs) as total_installs
        from {{ source("raw", "appsflyer_cost_channel") }}
        {% if is_incremental() %}
        -- 15-day window to ensure 14th day data is included before DAG runs
            where
                date(
                    execution_date
                ) between date_sub(date('{{ ds() }}'), interval 15 day) and date(
                    '{{ ds() }}'
                )
        {% endif %}
        group by
            app_install_date,
            acquisition_execution_date,
            app_os,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad
        qualify
            row_number() over (
                partition by
                    app_install_date,
                    app_os,
                    acquisition_media_source,
                    acquisition_campaign,
                    acquisition_adset,
                    acquisition_ad
                order by acquisition_execution_date desc
            )
            = 1
    ),

    -- Summarizes organic installs
    organic_installs as

    (
        select
            platform as app_os,
            media_source as acquisition_media_source,
            'None' as acquisition_campaign,
            'None' as acquisition_adset,
            'None' as acquisition_ad,
            date(install_time) as app_install_date,
            date(install_time) as acquisition_execution_date,
            0 as total_costs,
            count(*) as total_installs
        from {{ source("appsflyer_import", "installs") }}
        where media_source = 'organic'
        group by
            app_install_date,
            platform,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad,
            acquisition_execution_date
    )

select *
from campaign_installs
union all
select *
from organic_installs
