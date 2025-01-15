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
    app_id,
    media_source as acquisition_media_source,
    campaign as acquisition_campaign,
    adset as acquisition_adset,
    ad as acquisition_ad,
    version as acquisition_version,
    cast(days_post_attribution as int64) as acquisition_days_post_attribution,
    cast(conversion_date as date) as app_install_date,
    sum(
        if(event_name = 'af_complete_registration', cast(unique_users as int64), 0)
    ) as total_registrations,
    sum(
        if(event_name = 'af_complete_beneficiary', cast(unique_users as int64), 0)
    ) as total_beneficiaries,
    sum(
        if(event_name = 'af_complete_beneficiary_18', cast(unique_users as int64), 0)
    ) as total_beneficiaries_18,
    sum(
        if(
            event_name = 'af_complete_beneficiary_underage',
            cast(unique_users as int64),
            0
        )
    ) as total_beneficiaries_underage,
    sum(
        if(event_name = 'af_complete_beneficiary_17', cast(unique_users as int64), 0)
    ) as total_beneficiaries_17,
    sum(
        if(event_name = 'af_complete_beneficiary_16', cast(unique_users as int64), 0)
    ) as total_beneficiaries_16,
    sum(
        if(event_name = 'af_complete_beneficiary_15', cast(unique_users as int64), 0)
    ) as total_beneficiaries_15
from {{ source("appsflyer_import", "cohort_unified_timezone_versioned") }}
where
    cast(days_post_attribution as int64) < 14
    {% if is_incremental() %}
        and cast(conversion_date as date)
        between date_sub(date("{{ ds() }}"), interval 14 day) and date("{{ ds() }}")
    {% endif %}
group by
    app_install_date,
    app_id,
    acquisition_media_source,
    acquisition_campaign,
    acquisition_adset,
    acquisition_ad,
    acquisition_days_post_attribution,
    acquisition_version
qualify
    row_number() over (
        partition by
            app_install_date,
            app_id,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad,
            acquisition_days_post_attribution
        order by acquisition_version desc
    )
    = 1
