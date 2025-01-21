{{ config(pre_hook="{{ create_dehumanize_id_function() }}") }}

{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

WITH pro_event_raw_data AS (
  SELECT
      event_name,
      user_pseudo_id,
      user_id,
      platform,
      event_date,
      event_timestamp,
      ga_session_number AS session_number,
      ga_session_id AS session_id,
      CONCAT(user_pseudo_id, '-', ga_session_id) AS unique_session_id,
      origin,
      destination,
      traffic_campaign,
      traffic_medium,
      traffic_source,
      category AS user_device_category,
      operating_system AS user_device_operating_system,
      operating_system_version AS user_device_operating_system_version,
      browser AS user_web_browser,
      browser_version AS user_web_browser_version,
      COALESCE(CAST(offerer_id AS STRING), CAST(offererid AS STRING)) AS offerer_id,
      COALESCE(venue_id, CAST(venueid AS STRING)) AS venue_id,
      page_location,
      page_title AS page_name,
      REGEXP_EXTRACT(page_location, r"passculture\.pro\/([^\/\?]+)", 1) AS url_path_type,
      REGEXP_EXTRACT(page_location, r"passculture\.pro\/[^\/]+\/([^\/\?]+)", 1) AS url_path_details,
      JSON_EXTRACT_ARRAY(selected_offers) AS selected_offers_array,
      ARRAY_LENGTH(JSON_EXTRACT_ARRAY(selected_offers)) > 1 AS multiple_selection,
      COALESCE(CAST(double_offer_id AS STRING), CAST(offerid AS STRING)) AS offer_id,
      status AS offer_status,
      offerType AS offer_type
  from {{ ref("int_firebase__pro_event_flattened") }}
        {% if is_incremental() %}
            where
                event_date
                between date_sub(date("{{ ds() }}"), interval 2 day) and date(
                    "{{ ds() }}"
                )
        {% endif %}

),

unnested_events AS (
  SELECT
      *,
      JSON_EXTRACT_SCALAR(item, "$.offerId") AS offer_id_from_array,
      JSON_EXTRACT_SCALAR(item, "$.offerStatus") AS offer_status_from_array,
      multiple_selection AS is_multiple_selection
  FROM
      pro_event_raw_data, UNNEST(selected_offers_array) AS item
),

combined_events AS (
  SELECT
      event_name,
      user_pseudo_id,
      user_id,
      platform,
      event_date,
      event_timestamp,
      session_number,
      session_id,
      unique_session_id,
      origin,
      destination,
      traffic_campaign,
      traffic_medium,
      traffic_source,
      user_device_category,
      user_device_operating_system,
      user_device_operating_system_version,
      user_web_browser,
      user_web_browser_version,
      offerer_id,
      venue_id,
      page_location,
      page_name,
      url_path_type,
      url_path_details,
      is_multiple_selection AS multiple_selection, -- Utilisation du champ correct
      COALESCE(offer_id_from_array, offer_id) AS offer_id,
      COALESCE(offer_status_from_array, offer_status) AS offer_status,
      CAST(offer_type AS STRING) AS offer_type
  FROM
      unnested_events

  UNION ALL

  SELECT
      event_name,
      user_pseudo_id,
      user_id,
      platform,
      event_date,
      event_timestamp,
      session_number,
      session_id,
      unique_session_id,
      origin,
      destination,
      traffic_campaign,
      traffic_medium,
      traffic_source,
      user_device_category,
      user_device_operating_system,
      user_device_operating_system_version,
      user_web_browser,
      user_web_browser_version,
      offerer_id,
      venue_id,
      page_location,
      page_name,
      url_path_type,
      url_path_details,
      FALSE AS multiple_selection,
      CAST(offer_id AS STRING) AS offer_id,
      CAST(offer_status AS STRING) AS offer_status,
      CAST(offer_type AS STRING) AS offer_type
  FROM
      pro_event_raw_data
  WHERE
      selected_offers_array IS NULL
)

SELECT DISTINCT
    *
FROM
    combined_events


