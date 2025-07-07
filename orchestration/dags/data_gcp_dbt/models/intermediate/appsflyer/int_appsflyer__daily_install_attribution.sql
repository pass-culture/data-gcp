{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "app_install_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}


-- Aggregates user events, picking the latest version per app_install_date and
-- acquisition_days_post_attribution
select
    version as acquisition_version,
    safe_cast(days_post_attribution as int64) as acquisition_days_post_attribution,
    date(conversion_date) as app_install_date,
    case
        when app_id = 'app.passculture.webapp'
        then 'android'
        when app_id = 'id1557887412'
        then 'ios'
    end as app_os,
    coalesce(media_source, 'None') as acquisition_media_source,
    coalesce(campaign, 'None') as acquisition_campaign,
    coalesce(adset, 'None') as acquisition_adset,
    coalesce(ad, 'None') as acquisition_ad,
    sum(
        if(event_name = 'af_complete_registration', safe_cast(unique_users as int64), 0)
    ) as total_registrations,
    sum(
        if(event_name = 'af_complete_beneficiary', safe_cast(unique_users as int64), 0)
    ) as total_beneficiaries,
    sum(
        if(
            event_name = 'af_complete_beneficiary_18',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_beneficiaries_18,
    sum(
        if(
            event_name = 'af_complete_beneficiary_underage',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_beneficiaries_underage,
    sum(
        if(
            event_name = 'af_complete_beneficiary_17',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_beneficiaries_17,
    sum(
        if(
            event_name = 'af_complete_beneficiary_16',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_beneficiaries_16,
    sum(
        if(
            event_name = 'af_complete_beneficiary_15',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_beneficiaries_15,
    sum(
        if(
            event_name = 'af_complete_registration_15',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_registrations_15,
    sum(
        if(
            event_name = 'af_complete_registration_16',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_registrations_16,
    sum(
        if(
            event_name = 'af_complete_registration_17',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_registrations_17,
    sum(
        if(
            event_name = 'af_complete_registration_18',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_registrations_18,
    sum(
        if(
            event_name = 'af_complete_registration_19+',
            safe_cast(unique_users as int64),
            0
        )
    ) as total_registrations_19_plus
from {{ source("appsflyer_import", "cohort_unified_timezone_versioned") }}
where
    safe_cast(days_post_attribution as int64) < 14
    {% if is_incremental() %}
        and date(conversion_date)
        between date_sub(date('{{ ds() }}'), interval 14 day) and date('{{ ds() }}')
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
    row_number() over (
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
