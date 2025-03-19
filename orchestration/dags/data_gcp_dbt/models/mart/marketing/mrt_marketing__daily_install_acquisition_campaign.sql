{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "app_install_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}


with

    -- Consolidates user events by app_install_date
    daily_attribution_summary as (
        select
            app_install_date,
            app_os,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad,
            sum(total_registrations) as total_registrations,
            sum(total_beneficiaries) as total_beneficiaries,
            sum(total_beneficiaries_underage) as total_beneficiaries_underage,
            sum(total_beneficiaries_18) as total_beneficiaries_18,
            sum(total_beneficiaries_17) as total_beneficiaries_17,
            sum(total_beneficiaries_16) as total_beneficiaries_16,
            sum(total_beneficiaries_15) as total_beneficiaries_15,
            sum(total_registrations_15) as total_registrations_15,
            sum(total_registrations_16) as total_registrations_16,
            sum(total_registrations_17) as total_registrations_17,
            sum(total_registrations_18) as total_registrations_18,
            sum(total_registrations_19_plus) as total_registrations_19_plus
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
            app_os,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad
    )

select
    daily_attribution_summary.app_install_date,
    daily_attribution_summary.app_os,
    daily_attribution_summary.acquisition_media_source,
    daily_attribution_summary.acquisition_campaign,
    daily_attribution_summary.acquisition_adset,
    daily_attribution_summary.acquisition_ad,
    daily_install_cost.total_costs,
    daily_install_cost.total_installs,
    daily_attribution_summary.total_registrations,
    daily_attribution_summary.total_beneficiaries,
    daily_attribution_summary.total_beneficiaries_underage,
    daily_attribution_summary.total_beneficiaries_18,
    daily_attribution_summary.total_beneficiaries_17,
    daily_attribution_summary.total_beneficiaries_16,
    daily_attribution_summary.total_beneficiaries_15,
    daily_attribution_summary.total_registrations_15,
    daily_attribution_summary.total_registrations_16,
    daily_attribution_summary.total_registrations_17,
    daily_attribution_summary.total_registrations_18,
    daily_attribution_summary.total_registrations_19_plus
from daily_attribution_summary
left join
    {{ ref("int_appsflyer__daily_install_cost") }} as daily_install_cost
    on daily_install_cost.app_install_date = daily_attribution_summary.app_install_date
    and daily_install_cost.app_os = daily_attribution_summary.app_os
    and daily_install_cost.acquisition_media_source
    = daily_attribution_summary.acquisition_media_source
    and daily_install_cost.acquisition_campaign
    = daily_attribution_summary.acquisition_campaign
    and daily_install_cost.acquisition_adset
    = daily_attribution_summary.acquisition_adset
    and daily_install_cost.acquisition_ad = daily_attribution_summary.acquisition_ad
