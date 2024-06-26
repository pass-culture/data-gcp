{{
    config(
        materialized = "incremental" if target.profile != "CI" else "view",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "module_displayed_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

WITH module_context AS (
  SELECT
    DISTINCT 
      user_id,
      module_displayed_date,
      unique_session_id,
      user_location_type,
      entry_id,
      entry_name,
      module_id,
      module_name,
      parent_module_id,
      parent_module_type,
      parent_entry_id,
      parent_home_type,
      module_type,
      reco_call_id,
  FROM {{ ref('mrt_native__daily_user_home_module') }}
  {% if is_incremental() %}   
    WHERE module_displayed_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
  {% else %}
    WHERE module_displayed_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 60 DAY) and DATE('{{ ds() }}')
  {% endif %}
), offer_context AS (
    SELECT 
        doe.event_date as module_displayed_date, 
        mc.user_id,
        mc.unique_session_id,
        doe.reco_call_id,
        doe.playlist_origin, 
        doe.context,
        doe.offer_id,
        doe.offer_display_order,
        mc.user_location_type,
        mc.entry_id,
        mc.entry_name,
        mc.module_id,
        mc.module_name,
        mc.parent_module_id,
        mc.parent_module_type,
        mc.parent_entry_id,
        mc.parent_home_type,
        mc.module_type,
    FROM {{ ref('int_pcreco__displayed_offer_event') }} doe
    INNER JOIN module_context mc 
      ON 
      mc.module_displayed_date = doe.event_date
      AND mc.reco_call_id = doe.reco_call_id 
      AND mc.user_id = doe.user_id
    WHERE playlist_origin = "recommendation"
    {% if is_incremental() %}   
    AND event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
    {% else %}
    AND event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 60 DAY) and DATE('{{ ds() }}')
    {% endif %}
)

SELECT  
  oc.module_displayed_date, 
  oc.reco_call_id,
  oc.playlist_origin, 
  oc.context,
  oc.user_id,
  oc.offer_id,
  oc.offer_display_order,
  oc.user_location_type,
  oc.entry_id,
  oc.entry_name,
  oc.module_id,
  oc.module_name,
  oc.parent_module_id,
  oc.parent_module_type,
  oc.parent_entry_id,
  oc.parent_home_type,
  oc.module_type,
  mc.unique_session_id, 
  mc.booking_id,
  MAX(mc.consult_offer_timestamp) as consult_offer_timestamp,
  MAX(mc.booking_timestamp) as booking_timestamp,
  MAX(mc.fav_timestamp) as fav_timestamp, 

FROM offer_context oc
LEFT JOIN {{ ref('mrt_native__daily_user_home_module') }} mc
  ON oc.module_displayed_date = mc.module_displayed_date
  AND oc.reco_call_id = mc.reco_call_id 
  AND oc.playlist_origin = mc.module_type
  AND oc.offer_id = mc.offer_id
GROUP BY 
    module_displayed_date, 
    reco_call_id,
    playlist_origin, 
    context,
    user_id,
    offer_id,
    offer_display_order,
    user_location_type,
    entry_id,
    entry_name,
    module_id,
    module_name,
    parent_module_id,
    parent_module_type,
    parent_entry_id,
    parent_home_type,
    module_type,
    unique_session_id, 
    booking_id
