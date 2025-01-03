{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

-- Summarizes cost and installs, picking the latest execution per date
SELECT
  app_id,
  os,
  media_source,
  campaign,
  adset,
  ad,
  CAST(date AS DATE) AS install_date,
  CAST(execution_date AS DATE) AS execution_date,
  CAST(SUM(cost) AS INT64) AS total_costs,
  SUM(installs) AS total_installs
FROM {{ source("raw", "appsflyer_cost_channel") }}
{% if is_incremental() %}
WHERE CAST(execution_date AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
{% endif %}
GROUP BY install_date, execution_date, app_id, os, media_source, campaign, adset, ad
QUALIFY ROW_NUMBER() OVER (PARTITION BY install_date, app_id, os, media_source, campaign, adset, ad ORDER BY execution_date DESC) = 1
