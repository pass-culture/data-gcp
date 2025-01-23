{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "acquisition_execution_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

WITH

-- Summarizes campaign cost and installs, picking the latest execution per date
campaign_installs AS (
SELECT
    os AS app_os,
    media_source AS acquisition_media_source,
    IF(campaign = '', 'None', campaign) AS acquisition_campaign,
    IF(adset = '', 'None', adset) AS acquisition_adset,
    IF(ad = '', 'None', ad) AS acquisition_ad,
    CAST(date AS date) AS app_install_date,
    CAST(execution_date AS date) AS acquisition_execution_date,
    CAST(SUM(cost) AS int64) AS total_costs,
    SUM(installs) AS total_installs
FROM {{ source("raw", "appsflyer_cost_channel") }}
{% if is_incremental() %}
    WHERE
        CAST(execution_date AS date)
        BETWEEN DATE_SUB(DATE('{{ ds() }}'), INTERVAL 7 DAY) AND DATE('{{ ds() }}')
{% endif %}
GROUP BY
    app_install_date,
    acquisition_execution_date,
    app_os,
    acquisition_media_source,
    acquisition_campaign,
    acquisition_adset,
    acquisition_ad
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
            app_install_date,
            app_os,
            acquisition_media_source,
            acquisition_campaign,
            acquisition_adset,
            acquisition_ad
        ORDER BY acquisition_execution_date DESC
    )
    = 1),

-- Summarizes organic installs
organic_installs AS

(
SELECT
  platform AS app_os,
  media_source AS acquisition_media_source,
  'None' AS acquisition_campaign,
  'None' AS acquisition_adset,
  'None' AS acquisition_ad,
  DATE(install_time) AS app_install_date,
  DATE(install_time) AS acquisition_execution_date,
  0 AS total_costs,
  COUNT(*) AS total_installs
FROM `passculture-data-prod.appsflyer_import_prod.installs`
WHERE media_source = 'organic'
GROUP BY app_install_date, platform, acquisition_media_source, acquisition_campaign, acquisition_adset, acquisition_ad, acquisition_execution_date)


SELECT *
FROM campaign_installs
UNION ALL
SELECT *
FROM organic_installs
