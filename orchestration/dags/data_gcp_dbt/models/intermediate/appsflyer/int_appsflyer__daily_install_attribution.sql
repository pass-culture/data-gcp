{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "app_install_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}


-- Aggregates user events, picking the latest version per app_install_date and
-- acquisition_days_post_attribution
select
    version as acquisition_version,
    case
        when app_id = 'app.passculture.webapp' then 'android'
        when app_id = 'id1557887412' then 'ios'
    end as app_os,
    IF(media_source is null, 'None', media_source) as acquisition_media_source,
    IF(campaign is null, 'None', campaign) as acquisition_campaign,
    IF(adset is null, 'None', adset) as acquisition_adset,
    IF(ad is null, 'None', ad) as acquisition_ad,
    CAST(days_post_attribution as int64) as acquisition_days_post_attribution,
    CAST(conversion_date as date) as app_install_date,
    SUM(
        IF(event_name = 'af_complete_registration', CAST(unique_users as int64), 0)
    ) as total_registrations,
    SUM(
        IF(event_name = 'af_complete_beneficiary', CAST(unique_users as int64), 0)
    ) as total_beneficiaries,
    SUM(
        IF(event_name = 'af_complete_beneficiary_18', CAST(unique_users as int64), 0)
    ) as total_beneficiaries_18,
    SUM(
        IF(
            event_name = 'af_complete_beneficiary_underage',
            CAST(unique_users as int64),
            0
        )
    ) as total_beneficiaries_underage,
    SUM(
        IF(event_name = 'af_complete_beneficiary_17', CAST(unique_users as int64), 0)
    ) as total_beneficiaries_17,
    SUM(
        IF(event_name = 'af_complete_beneficiary_16', CAST(unique_users as int64), 0)
    ) as total_beneficiaries_16,
    SUM(
        IF(event_name = 'af_complete_beneficiary_15', CAST(unique_users as int64), 0)
    ) as total_beneficiaries_15
from {{ source("appsflyer_import", "cohort_unified_timezone_versioned") }}
where
    CAST(days_post_attribution as int64) < 14
    {% if is_incremental() %}
        and CAST(conversion_date as date)
        between DATE_SUB(DATE('{{ ds() }}'), interval 14 day) and DATE('{{ ds() }}')
    {% endif %}
group by
    app_install_date,
    app_os,
    acquisition_media_source,
    acquisition_campaign,
    acquisition_adset,
    acquisition_ad,
    acquisition_days_post_attribution,
    acquisition_version
qualify
    ROW_NUMBER() over (
        partition by
            app_install_date,
            app_os,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad,
            acquisition_days_post_attribution
        order by acquisition_version desc
    )
    = 1
