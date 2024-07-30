SELECT
  rank_execution.card_id
  , rank_execution.card_name
  , rank_execution.card_creation_date
  , rank_execution.card_update_date
  , rank_execution.card_collection_id
  , location
  , avg_running_time
  , avg_result_rows
  , total_users
  , total_views
  , nbr_dashboards
  , last_execution_date
  , context as last_execution_context
  , total_errors
  , CASE
      WHEN location like '/610%' or card_collection_id = 610 THEN 'archive'
      WHEN location like '/607%' THEN 'externe'
      WHEN location like '/606/614%' or card_collection_id = 614 THEN 'operationnel'  
      WHEN location like '/606/615%' or card_collection_id = 615 THEN 'adhoc' 
      WHEN location like '/608%' THEN 'interne'
      ELSE 'other'
    END as parent_folder
   , ref_archive.*
FROM {{ ref('int_metabase__daily_query') }} rank_execution
JOIN {{ ref('int_metabase__aggregated_card_activity') }} aggregated_activity
ON rank_execution.card_id = aggregated_activity.card_id
JOIN {{ source("raw", "metabase_collection") }}  public_collections
  ON public_collections.collection_id = rank_execution.card_collection_id
JOIN {{ ref('int_metabase__collection_archive') }} as ref_archive
    ON rank_execution.card_collection_id = ref_archive.collection_id
WHERE rank_execution.card_id_execution_rank = 1