WITH rank_execution as (
SELECT 
  card_id
  , context
  , execution_date
  , row_number() over(partition by card_id order by execution_date desc) as rank
FROM {{ source("raw", "metabase_query_execution") }} as execution_query
order by execution_date desc
),

aggregated_activity as (
  SELECT 
        card_id
        , avg(running_time) as avg_running_time
        , avg(result_rows) as avg_result_rows
        , count(distinct executor_id) as total_users
        , count(distinct execution_query.execution_id) as total_views
        , count(distinct dashboard_id) as nbr_dashboards
        , max(execution_query.execution_date) as last_execution_date
        , sum(case when error is null then 0 else 1 end) as total_errors
    FROM  {{ source("raw", "metabase_query_execution") }} as execution_query
    GROUP BY 
      card_id
)
SELECT
  rank_execution.card_id
  , report_card.card_name as card_name
  , report_card.created_at as card_creation_date
  , report_card.updated_at as card_update_date
  , report_card.card_collection_id as card_collection_id
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
FROM rank_execution
JOIN aggregated_activity
ON rank_execution.card_id = aggregated_activity.card_id
JOIN {{ source("raw", "metabase_report_card") }} as report_card
  ON aggregated_activity.card_id = report_card.id
JOIN {{ source("raw", "metabase_collection") }}  public_collections
  ON public_collections.collection_id = report_card.card_collection_id
JOIN {{ ref('int_metabase__collection_archive') }} as ref_archive
    ON report_card.card_collection_id = ref_archive.collection_id
WHERE rank = 1