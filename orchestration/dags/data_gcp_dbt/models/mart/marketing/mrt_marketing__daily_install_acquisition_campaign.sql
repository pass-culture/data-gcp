{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "app_install_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}


with

    -- Consolidates user events by app_install_date
    daily_attribution_summary as (
        select
            app_install_date,
            app_id,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad,
            sum(registration) as total_registrations,
            sum(beneficiary) as total_beneficiaries,
            sum(beneficiary_underage) as total_beneficiaries_underage,
            sum(beneficiary_18) as total_beneficiaries_18,
            sum(beneficiary_17) as total_beneficiaries_17,
            sum(beneficiary_16) as total_beneficiaries_16,
            sum(beneficiary_15) as total_beneficiaries_15
        from {{ ref("int_appsflyer__daily_install_attribution") }}
        {% if is_incremental() %}
            where
                app_install_date
                between date_sub(date("{{ ds() }}"), interval 14 day) and date(
                    "{{ ds() }}"
                )
        {% endif %}
        group by
            app_install_date,
            app_id,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad
    )

select
    daily_install_cost.app_install_date,
    daily_install_cost.app_os,
    daily_install_cost.acquisition_media_source,
    daily_install_cost.acquisition_campaign,
    daily_install_cost.acquisition_adset,
    daily_install_cost.acquisition_ad,
    daily_install_cost.total_costs,
    daily_install_cost.total_installs,
    daily_attribution_summary.total_registrations,
    daily_attribution_summary.total_beneficiaries,
    daily_attribution_summary.total_beneficiaries_underage,
    daily_attribution_summary.total_beneficiaries_18,
    daily_attribution_summary.total_beneficiaries_17,
    daily_attribution_summary.total_beneficiaries_16,
    daily_attribution_summary.total_beneficiaries_15
from {{ ref("int_appsflyer__daily_install_cost") }} as daily_install_cost
inner join
    daily_attribution_summary
    on daily_install_cost.app_install_date = daily_attribution_summary.app_install_date
    and daily_install_cost.app_id = daily_attribution_summary.app_id
    and daily_install_cost.acquisition_media_source
    = daily_attribution_summary.acquisition_media_source
    and daily_install_cost.acquisition_campaign
    = daily_attribution_summary.acquisition_campaign
    and daily_install_cost.acquisition_adset
    = daily_attribution_summary.acquisition_adset
    and daily_install_cost.acquisition_ad = daily_attribution_summary.acquisition_ad
