WITH metabase_query AS (
    SELECT 
        mqe.execution_date,
        TO_HEX(`hash`) as metabase_hash, 
        dashboard_id,
        mqe.card_id,
        mqe.execution_id,
        mqe.executor_id as metabase_user_id,
        mqe.cache_hit,
        mqe.error,
        mqe.context,
        sum(mqe.running_time) as running_time,
        sum(mqe.result_rows) as result_rows
    FROM {{ source("raw", "metabase_query_execution") }}  mqe
    GROUP BY execution_date,
        metabase_hash,
        dashboard_id,
        card_id,
        execution_id,
        metabase_user_id,
        cache_hit,
        error,
        context
) 

SELECT
    mqe.*,
    mrc.card_name,
    mrc.created_at AS card_creation_date,
    mrc.updated_at AS card_update_date,
    mrc.card_collection_id as card_collection_id,
    mrd.dashboard_name,
    row_number() over(partition by card_id order by execution_date desc) as card_id_execution_rank
FROM metabase_query AS mqe
INNER JOIN  {{ source("raw", "metabase_report_card") }} AS mrc ON mqe.card_id = mrc.id
LEFT JOIN {{ source("raw", "metabase_report_dashboard") }} AS mrd  ON mqe.dashboard_id = mrd.id