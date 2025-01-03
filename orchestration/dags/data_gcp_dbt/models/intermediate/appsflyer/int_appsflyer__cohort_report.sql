{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "install_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

WITH

-- Aggregates user events, picking the latest version per install_date and days_post_attribution
cohort_unified AS (
  SELECT
    app_id,
    media_source,
    campaign,
    adset,
    ad,
    days_post_attribution,
    version,
    CAST(conversion_date AS DATE) AS install_date,
    SUM(IF(event_name = 'af_complete_registration', CAST(unique_users AS INT64), 0)) AS registration,
    SUM(IF(event_name = 'af_complete_beneficiary', CAST(unique_users AS INT64), 0)) AS beneficiary,
    SUM(IF(event_name = 'af_complete_beneficiary_18', CAST(unique_users AS INT64), 0)) AS beneficiary_18,
    SUM(IF(event_name = 'af_complete_beneficiary_underage', CAST(unique_users AS INT64), 0)) AS beneficiary_underage,
    SUM(IF(event_name = 'af_complete_beneficiary_17', CAST(unique_users AS INT64), 0)) AS beneficiary_17,
    SUM(IF(event_name = 'af_complete_beneficiary_16', CAST(unique_users AS INT64), 0)) AS beneficiary_16,
    SUM(IF(event_name = 'af_complete_beneficiary_15', CAST(unique_users AS INT64), 0)) AS beneficiary_15
FROM {{ source("appsflyer_import", "cohort_unified_timezone_versioned") }}
{% if is_incremental() %}
WHERE CAST(conversion_date AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
{% endif %}
GROUP BY install_date, app_id, media_source, campaign, adset, ad, days_post_attribution, version
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY install_date, app_id, media_source, campaign,  adset, ad, days_post_attribution ORDER BY version DESC) = 1)

-- Consolidates user events by install_date
SELECT
    install_date,
    app_id,
    media_source,
    campaign,
    adset,
    ad,
    SUM(registration) AS total_registrations,
    SUM(beneficiary) AS total_beneficiaries,
    SUM(beneficiary_underage) AS total_beneficiaries_underage,
    SUM(beneficiary_18) AS total_beneficiaries_18,
    SUM(beneficiary_17) AS total_beneficiaries_17,
    SUM(beneficiary_16) AS total_beneficiaries_16,
    SUM(beneficiary_15) AS total_beneficiaries_15
FROM cohort_unified
GROUP BY install_date, app_id, media_source, campaign, adset, ad
