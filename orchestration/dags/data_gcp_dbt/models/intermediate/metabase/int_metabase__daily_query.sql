WITH metabase_query AS (
    SELECT 
        execution_date,
        TO_HEX(`hash`) as metabase_hash, 
        dashboard_id,
        card_id,
        execution_id,
        executor_id as metabase_user_id,
        mrc.card_name,
        mrc.created_at as card_creation_date,
        mrc.updated_at as card_update_date,
        mrc.card_collection_id as card_collection_id,
        mrd.dashboard_name,
        mqe.cache_hit,
        mqe.error,
        mqe.context,
        sum(running_time) as running_time,
        sum(result_rows) as result_rows
    FROM {{ source("raw", "metabase_query_execution") }}  mqe
    JOIN  {{ source("raw", "metabase_report_card") }}  mrc ON mqe.card_id = mrc.id
    LEFT JOIN {{ source("raw", "metabase_report_dashboard") }} mrd  ON mqe.dashboard_id = mrd.id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) 

SELECT
    *,
    row_number() over(partition by card_id order by execution_date desc) as card_id_execution_rank
FROM metabase_query