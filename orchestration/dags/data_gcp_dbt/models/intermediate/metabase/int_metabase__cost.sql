
WITH metabase_cost AS (
    SELECT
        creation_date, 
        metabase_hash,
        metabase_user_id,
        SUM(total_bytes) as total_bytes,
        SUM(total_queries) as total_queries,
        SUM(cost_usd) as cost_usd
    FROM ref('int_gcp__bigquery_cost')
    GROUP BY creation_date, metabase_hash, metabase_user_id
),


metabase_query AS (
    SELECT 
        date(execution_date) as date,
        TO_HEX(`hash`) as metabase_hash, 
        dashboard_id,
        card_id,
        executor_id as metabase_user_id,
        mrc.card_name,
        mrd.dashboard_name,
        sum(running_time) as running_time,
        sum(result_rows) as result_rows

    FROM {{ source("raw", "metabase_query_execution") }}  mqe
    JOIN  {{ source("raw", "metabase_report_card") }}  mrc ON mqe.card_id = mrc.id
    AND NOT mqe.cache_hit 
    LEFT JOIN {{ source("raw", "metabase_report_dashboard") }} mrd  ON mqe.dashboard_id = mrd.id
    GROUP BY 1,2,3,4,5, 6, 7

)


SELECT 
    mq.date as date,
    mq.metabase_hash, 
    mq.dashboard_id,
    mq.card_id,
    mq.metabase_user_id,
    mq.card_name,
    mq.dashboard_name,
    result_rows,
    running_time,
    total_bytes,
    total_queries, 
    cost_usd
FROM metabase_query mq 
LEFT JOIN metabase_cost mc on mc.creation_date = mq.date 
    AND mc.metabase_hash = mq.metabase_hash
    AND CAST(mc.metabase_user_id AS INT) = mq.metabase_user_id