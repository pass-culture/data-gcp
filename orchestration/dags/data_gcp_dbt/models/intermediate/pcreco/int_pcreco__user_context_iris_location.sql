{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
        on_schema_change = "sync_all_columns"
    )
}}

SELECT
  event_date,
  user_id,
  user_context.user_iris_id,
  COUNT(DISTINCT reco_call_id) as total_events,
FROM {{ ref("int_pcreco__recommended_offer_event")}} pso
WHERE 
user_context.user_iris_id IS NOT NULL 
AND user_context.user_is_geolocated
AND user_id != "-1"

{% if is_incremental() %}   
AND event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}') 
{% endif %}
  

GROUP BY event_date,
    user_id,
    user_iris_id