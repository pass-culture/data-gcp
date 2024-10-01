{{
  config(
    **custom_incremental_config(
    partition_by={
      "field": "first_event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = "sync_all_columns"
  )
) }}

-- TODO (legacy) : remplacer ici firebase_visits par un modèle int_firebase__native_session
-- merge firebase_visits et firebase_session_origin dans ce modèle (mrt_native__session)

with consultation_by_sessions as (
SELECT
    unique_session_id,
    SUM(item_discovery_score) AS item_discovery_score,
    SUM(subcategory_discovery_score) AS subcategory_discovery_score,
    SUM(category_discovery_score) AS category_discovery_score
from {{ ref('mrt_native__consultation')}}
group by unique_session_id
)

SELECT
    v.unique_session_id,
    v.user_pseudo_id,
    v.unique_session_id,
    v.platform,
    v.app_version,
    v.session_number,
    v.user_id,
    v.first_event_date,
    v.traffic_campaign,
    v.traffic_medium,
    v.traffic_source,
    s.item_discovery_score,
    s.subcategory_discovery_score,
    s.category_discovery_score
FROM {{ ref('firebase_visits') }} AS v
LEFT JOIN consultation_by_sessions AS s ON s.unique_session_id = v.unique_session_id
{% if is_incremental() %}
WHERE event_date between DATE_SUB(DATE('{{ ds() }}'), interval 1 day) and DATE('{{ ds() }}') and
{% endif %}
