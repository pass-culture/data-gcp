
WITH metabase_cost AS (
    SELECT
        creation_date, 
        metabase_hash,
        metabase_user_id,
        SUM(total_bytes) as total_bytes,
        SUM(total_queries) as total_queries,
        SUM(cost_usd) as cost_usd
    FROM {{ ref('int_gcp__bigquery_cost') }}
    GROUP BY creation_date, metabase_hash, metabase_user_id
     
)

SELECT 
    date(mq.execution_date) as date,
    mq.metabase_hash, 
    mq.dashboard_id,
    mq.card_id,
    mq.metabase_user_id,
    mq.card_name,
    mq.dashboard_name,
    avg(result_rows) as mean_result_rows,
    sum(total_bytes) as total_bytes,
    sum(total_queries) as total_queries, 
    sum(cost_usd) as cost_usd
FROM {{ ref("int_metabase__daily_query") }} mq 
INNER JOIN metabase_cost mc 
    ON date(mc.creation_date) = date(mq.execution_date)
    AND mc.metabase_hash = mq.metabase_hash
    AND mc.metabase_user_id = mq.metabase_user_id
WHERE NOT cache_hit -- only thoses where we have real SQL queries
GROUP BY 
    date(mq.execution_date),
    mq.metabase_hash, 
    mq.dashboard_id,
    mq.card_id,
    mq.metabase_user_id,
    mq.card_name,
    mq.dashboard_name