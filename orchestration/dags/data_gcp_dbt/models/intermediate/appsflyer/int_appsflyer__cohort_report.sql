{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "install_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

with

    -- Aggregates user events, picking the latest version per install_date and
    -- days_post_attribution
    cohort_unified as (
        select
            app_id,
            media_source,
            campaign,
            adset,
            ad,
            days_post_attribution,
            version,
            cast(conversion_date as date) as install_date,
            sum(
                if(
                    event_name = 'af_complete_registration',
                    cast(unique_users as int64),
                    0
                )
            ) as registration,
            sum(
                if(
                    event_name = 'af_complete_beneficiary',
                    cast(unique_users as int64),
                    0
                )
            ) as beneficiary,
            sum(
                if(
                    event_name = 'af_complete_beneficiary_18',
                    cast(unique_users as int64),
                    0
                )
            ) as beneficiary_18,
            sum(
                if(
                    event_name = 'af_complete_beneficiary_underage',
                    cast(unique_users as int64),
                    0
                )
            ) as beneficiary_underage,
            sum(
                if(
                    event_name = 'af_complete_beneficiary_17',
                    cast(unique_users as int64),
                    0
                )
            ) as beneficiary_17,
            sum(
                if(
                    event_name = 'af_complete_beneficiary_16',
                    cast(unique_users as int64),
                    0
                )
            ) as beneficiary_16,
            sum(
                if(
                    event_name = 'af_complete_beneficiary_15',
                    cast(unique_users as int64),
                    0
                )
            ) as beneficiary_15
        from {{ source("appsflyer_import", "cohort_unified_timezone_versioned") }}
        {% if is_incremental() %}
            where
                cast(conversion_date as date)
                between date_sub(current_date(), interval 7 day) and current_date()
        {% endif %}
        group by
            install_date,
            app_id,
            media_source,
            campaign,
            adset,
            ad,
            days_post_attribution,
            version
        qualify
            row_number() over (
                partition by
                    install_date,
                    app_id,
                    media_source,
                    campaign,
                    adset,
                    ad,
                    days_post_attribution
                order by version desc
            )
            = 1
    )

-- Consolidates user events by install_date
select
    install_date,
    app_id,
    media_source,
    campaign,
    adset,
    ad,
    sum(registration) as total_registrations,
    sum(beneficiary) as total_beneficiaries,
    sum(beneficiary_underage) as total_beneficiaries_underage,
    sum(beneficiary_18) as total_beneficiaries_18,
    sum(beneficiary_17) as total_beneficiaries_17,
    sum(beneficiary_16) as total_beneficiaries_16,
    sum(beneficiary_15) as total_beneficiaries_15
from cohort_unified
group by install_date, app_id, media_source, campaign, adset, ad
